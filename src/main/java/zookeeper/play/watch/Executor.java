package zookeeper.play.watch;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.istack.internal.Nullable;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * A simple example program to use DataMonitor to start and
 * stop executables based on a znode. The program watches the
 * specified znode and saves the data that corresponds to the
 * znode in the filesystem. It also starts the specified program
 * with the specified arguments when the znode exists and kills
 * the program if the znode goes away.
 *
 * This will act somewhat like a Controller from MVC.
 */
public class Executor implements Watcher, Runnable, DataMonitor.DataMonitorListener
{
	/**
	 * Main Entry Point for program
	 * @param args hostport znode logFile program [args ...]
	 */
	public static void main(String[] args) {

		if (args.length < 4) {
			System.err.println("USAGE: Executor hostPort znode logFile program [args ...]");
			System.exit(2);
		}

		String hostPort = args[0];
		String znode = args[1];
		String filename = args[2];
		String exec[] = new String[args.length - 3];
		System.arraycopy(args, 3, exec, 0, exec.length);

		try {
			new Executor(hostPort, znode, filename, exec).run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Simple class object that takes an input and output stream and
	 * transfers the input to the output in a separate thread
	 */
	static class StreamWriter implements Runnable {
		OutputStream os;
		InputStream is;

		StreamWriter(InputStream is, OutputStream os) {
			this.is = is;
			this.os = os;
		}

		@Override
		public void run() {
			byte b[] = new byte[80];
			int rc;
			try {
				while ((rc = is.read(b)) > 0) {
					os.write(b, 0, rc);
				}
			} catch (IOException e) {
				System.out.println("Stream interruption captured in Stream Writer. Program context most likely switching...");
			}
		}
	}

	private ZooKeeper zk;
	private DataMonitor dm;

	private String logFile;
	private String execution[];

	private ExecutorService threadPool;
	private Process child;

	public Executor(String hostPort, String znode, String filename, String exec[])
	throws KeeperException, IOException
	{
		this.logFile = filename;
		this.execution = exec;
		this.threadPool = Executors.newFixedThreadPool(4);

		// Tell ZooKeeper to use the Executor class for all watch callbacks
		// This will route the watch callbacks to the data monitor, which can only be created after
		// the Zookeeper client. In order to break the circular dependency we'll tell ZooKeeper
		// to use the Executor class for all watch callbacks. This will route the watch callbacks to ... etc etc etc
		zk = new ZooKeeper(hostPort, 3000, this);

		dm = new DataMonitor(zk, znode, null, this);
	}

	@Override
	public void run() {
		try {
			synchronized (this) {
				// Event processing loop
				while (!dm.isDead()) {
					System.out.println("Entering Event Loop");
					wait();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		if(zk.getState() == ZooKeeper.States.CONNECTED) {
			try {
				zk.close();
			} catch (InterruptedException ie) {
				throw new RuntimeException(ie);
			}
		}
	}

	/**
	 * We do not process any events ourselves, we just need to forward them on.
	 *
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(WatchedEvent event) {
		dm.process(event);
	}

	@Override
	public void closing(int rc) {
		synchronized (this) {
			System.out.println("Notifying Event Loop for Shutdown");
			notifyAll();
		}
	}

	/**
	 * Carry out a task based on the data returned from the data monitor.
	 *
	 * @param data bytes of data contained in ZNode.
	 */
	@Override
	public void processExistsStatusChangeEvent(@Nullable byte[] data) {
		if (data == null) {

			// Node does not exist

			if (child != null) {
				System.out.println("Killing process");
				child.destroy();
				try {
					child.waitFor();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			child = null;

		} else {

			// Node Exists and has changed.

			if (child != null) {
				System.out.println("Stopping child");
				child.destroy();
				try {
					child.waitFor();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			// Log the znode data to a file.
			try {
				FileOutputStream fos = new FileOutputStream(logFile, true);
				fos.write(data);
				fos.write("\n".getBytes());
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			try {
				System.out.println("Starting child");
				child = Runtime.getRuntime().exec(execution);

				// Use a set of threads to continuously pipe the child stdout streams to this process's stdout
				threadPool.execute(new StreamWriter(child.getInputStream(), System.out));
				threadPool.execute(new StreamWriter(child.getErrorStream(), System.err));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}