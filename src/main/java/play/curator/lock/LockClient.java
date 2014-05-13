package play.curator.lock;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class LockClient {

	public static final String NAMESPACE = "test/locks";
	public static final String LOCK_NODE = "lockClient1";

	/**
	 * Starts a process to test locking across Zookeeper using Curator
	 * @param args hostport waitTime processTime
	 * @throws Exception
	 */
	public static void main(String[] args)
	throws Exception
	{
		new LockClient(args[0], Long.parseLong(args[1]), Long.parseLong(args[2])).run();
	}

	private String connectionString;
	private long waitTime;
	private long processTime;

	public LockClient(String connectionString, long waitTime, long processTime) {
		this.connectionString = connectionString;
		this.waitTime = waitTime;
		this.processTime = processTime;
	}

	public void run()
	throws Exception
	{
		CuratorFramework framework = null;

		try {
			framework = CuratorFrameworkFactory.builder()
					.connectString(connectionString)
					.connectionTimeoutMs(3000)
					.namespace(NAMESPACE)
					.retryPolicy(new ExponentialBackoffRetry(1000, 3))
					.build();

			framework.start();

			InterProcessLock lock = new InterProcessSemaphoreMutex(framework, LOCK_NODE);

			System.out.println("Locking...");
			if(lock.acquire(waitTime, TimeUnit.MILLISECONDS)) {
				try {
					System.out.println("Aquired Lock!");
					System.out.println("Beginning 'Processing' ...");
					Thread.sleep(processTime);
					System.out.println("'Processing' finished...");
				} finally {
					lock.release();
					System.out.println("Lock Released");
				}
			} else {
				System.out.println("Could not aquire lock. Exiting...");
			}

		} finally {
			if(framework != null) {
				framework.close();
			}
		}
	}
}
