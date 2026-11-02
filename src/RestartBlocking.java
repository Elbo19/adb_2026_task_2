import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Based on Blocking.java by Martin Schaeler.
 * Change: transactions aborted by the DBMS (deadlock / serialization failure)
 * are rolled back and restarted from the client side until they commit.
 */
public class RestartBlocking {
    public static int num_taks = 30;
    public static int num_queries_per_task = 3;
    public static int num_threads = 4;

    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;
    AtomicInteger tasks_finished = new AtomicInteger(0);
    AtomicInteger total_aborts = new AtomicInteger(0);

    void executeSQL(String sql, Connection con) {
        try (java.sql.Statement stmt = con.createStatement()) {
            stmt.execute(sql);
            System.out.println("Executing " + sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void createDB() {
        try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
            executeSQL("DROP TABLE IF EXISTS blocking_data;", con);
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);", con);

            con.setAutoCommit(false);
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)")) {
                for (int i = 0; i < num_tuple; i++) {
                    ps.setInt(1, i);
                    ps.setDouble(2, rand.nextDouble() * max_value);
                    ps.setInt(3, 0);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            con.commit();
            System.out.println("Inserted " + num_tuple + " tuples.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /** Errors after which the transaction can safely be retried. */
    static boolean isRetryable(SQLException e) {
        String state = e.getSQLState();
        return "40P01".equals(state)   // deadlock_detected
                || "40001".equals(state);  // serialization_failure
    }

    public static void main(String[] args) {
        RestartBlocking b = new RestartBlocking();
        b.createDB();

        ArrayList<Blocker> my_tasks = new ArrayList<>(num_taks);
        for (int task = 0; task < num_taks; task++) {
            my_tasks.add(b.new Blocker(task));
        }

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(num_threads);
        double start = System.currentTimeMillis();
        for (Blocker task : my_tasks) {
            executor.execute(task);
        }
        executor.shutdown();

        while (!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        double stop = System.currentTimeMillis();
        System.out.println("[DONE] Task execution after roughly " + (stop - start) + " ms finished: "
                + b.tasks_finished + " of " + num_taks + ", total restarts: " + b.total_aborts);
    }

    public class Blocker implements Runnable {
        final int task;
        final double[] start_range;
        final double[] stop_range;

        public Blocker(int id) {
            this.task = id;
            this.start_range = new double[num_queries_per_task];
            this.stop_range = new double[num_queries_per_task];
            for (int q = 0; q < num_queries_per_task; q++) {
                stop_range[q] = rand.nextDouble() * max_value;
                start_range[q] = rand.nextDouble() * stop_range[q];
            }
        }

        /** One attempt of the transaction: the same updates as in Blocking.java. */
        private void executeTransaction(Connection con) throws SQLException {
            try (PreparedStatement ps = con.prepareStatement(
                    "UPDATE blocking_data SET data_value = ?, modified_by = " + task
                            + " WHERE data_value >= ? AND data_value <= ?;",
                    ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
                for (int q = 0; q < num_queries_per_task; q++) {
                    ps.setDouble(1, (float) (rand.nextDouble() * max_value));
                    ps.setDouble(2, start_range[q]);
                    ps.setDouble(3, stop_range[q]);
                    ps.execute();
                }
            }
        }

        @Override
        public void run() {
            Connection con = null; // one connection per task, reused across retries
            int attempt = 0;
            try {
                con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                con.setAutoCommit(false);
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                while (true) {
                    attempt++;
                    try {
                        executeTransaction(con);
                        con.commit();
                        int num_finished = tasks_finished.incrementAndGet();
                        System.out.println("Done task " + task + " after " + attempt
                                + " attempt(s). I am finisher number " + num_finished);
                        return;
                    } catch (SQLException e) {
                        try {
                            con.rollback(); // reset the aborted transaction
                        } catch (SQLException rollbackError) {
                            System.err.println("Task " + task + " rollback failed: " + rollbackError.getMessage());
                            return;
                        }
                        if (isRetryable(e)) {
                            total_aborts.incrementAndGet();
                            System.err.println("Task " + task + " aborted on attempt " + attempt
                                    + " (SQLState " + e.getSQLState() + "), restarting");
                        } else {
                            System.err.println("Task " + task + " failed with non-retryable error: " + e.getMessage());
                            return;
                        }
                    }
                }
            } catch (SQLException e) {
                System.err.println("Task " + task + " could not set up connection: " + e.getMessage());
            } finally {
                if (con != null) {
                    try {
                        con.close();
                    } catch (SQLException ignored) {
                    }
                }
            }
        }
    }
}