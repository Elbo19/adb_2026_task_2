import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Based on Blocking.java by Martin Schaeler.
 * Starvation-free restart strategy:
 *  1. Aborted transactions are rolled back and restarted (as in RestartBlocking).
 *  2. Before each retry the task waits a random, exponentially growing time (backoff with jitter).
 *  3. After max_optimistic_attempts failures, the next attempt first takes an EXCLUSIVE
 *     table lock and runs without concurrent writers. That attempt cannot deadlock,
 *     so every task commits after at most max_optimistic_attempts + 1 attempts.
 */
public class NoLiveLocks {
    public static int num_taks = 30;
    public static int num_queries_per_task = 3;
    public static int num_threads = 4;

    /** public static int num_taks = 60;
    public static int num_queries_per_task = 8;
    public static int num_threads = 12;**/

    public static int max_optimistic_attempts = 3; // K
    public static long base_backoff_ms = 50;
    public static long max_backoff_ms = 2000;

    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;
    AtomicInteger tasks_finished = new AtomicInteger(0);
    AtomicInteger total_aborts = new AtomicInteger(0);
    AtomicInteger escalations = new AtomicInteger(0);
    AtomicInteger max_attempts = new AtomicInteger(0);

    void executeSQL(String sql, Connection con) {
        try (Statement stmt = con.createStatement()) {
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

    static boolean isRetryable(SQLException e) {
        String state = e.getSQLState();
        return "40P01".equals(state)   // deadlock_detected
                || "40001".equals(state);  // serialization_failure
    }

    /** Random exponential backoff: wait between 0 and min(max, base * 2^(attempt-1)) ms. */
    static void backoff(int attempt) {
        long cap = Math.min(max_backoff_ms, base_backoff_ms << Math.min(attempt - 1, 20));
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(cap + 1));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        NoLiveLocks b = new NoLiveLocks();
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
                + b.tasks_finished + " of " + num_taks
                + ", total restarts: " + b.total_aborts
                + ", escalations: " + b.escalations
                + ", max attempts of a single task: " + b.max_attempts);
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
            Connection con = null;
            int attempt = 0;
            try {
                con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                con.setAutoCommit(false);
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                while (true) {
                    attempt++;
                    boolean exclusive = attempt > max_optimistic_attempts;
                    try {
                        if (exclusive) {
                            if (attempt == max_optimistic_attempts + 1) {
                                escalations.incrementAndGet();
                                System.out.println("Task " + task + " escalates to exclusive mode on attempt " + attempt);
                            }
                            try (Statement st = con.createStatement()) {
                                st.execute("LOCK TABLE blocking_data IN EXCLUSIVE MODE");
                            }
                        }
                        executeTransaction(con);
                        con.commit(); // also releases the table lock
                        int num_finished = tasks_finished.incrementAndGet();
                        max_attempts.accumulateAndGet(attempt, Math::max);
                        System.out.println("Done task " + task + " after " + attempt + " attempt(s)"
                                + (exclusive ? " [exclusive]" : "") + ". I am finisher number " + num_finished);
                        return;
                    } catch (SQLException e) {
                        try {
                            con.rollback();
                        } catch (SQLException rollbackError) {
                            System.err.println("Task " + task + " rollback failed: " + rollbackError.getMessage());
                            return;
                        }
                        if (isRetryable(e)) {
                            total_aborts.incrementAndGet();
                            System.err.println("Task " + task + " aborted on attempt " + attempt
                                    + " (SQLState " + e.getSQLState() + "), backing off and restarting");
                            backoff(attempt);
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