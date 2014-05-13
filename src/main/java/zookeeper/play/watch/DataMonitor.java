package zookeeper.play.watch;

import java.util.Arrays;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

/**
 * A simple class that monitors the data and existence of a ZooKeeper
 * node. It uses asynchronous ZooKeeper APIs.
 *
 * This will act somewhat like a View from MVC.
 */
public class DataMonitor implements Watcher, StatCallback {

	private ZooKeeper zk;

	private String znode;

	private Watcher chainedWatcher;

	private boolean dead;

	private DataMonitorListener listener;

	// The data that existed at the ZNode on the last call
	private byte prevData[];

	public DataMonitor(ZooKeeper zk, String znode, Watcher chainedWatcher, DataMonitorListener listener)
	{
		this.zk = zk;
		this.znode = znode;
		this.chainedWatcher = chainedWatcher;
		this.listener = listener;
		// Get things started by checking if the node exists. We are going
		// to be completely event driven

		// This object will be the stat call back but the executor will be the watcher
		zk.exists(znode, true, this, null);
	}

	/**
	 * Interface used for the Controller and Event processor for the data monitor "view"
	 */
	public interface DataMonitorListener {
		/**
		 * The existence status of the node has changed.
		 */
		void processExistsStatusChangeEvent(byte data[]);

		/**
		 * Zookeeper session is no longer valid, Close the program.
		 *
		 * @param rc the ZooKeeper reason code
		 */
		void closing(int rc);
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		System.out.println("Received Watch Event!");
		if (event.getType() == Event.EventType.None) {
			// We are are being told that the state of the
			// connection has changed
			System.out.println("Recieved Event of Type none - Status : " + event.getState());
			switch (event.getState()) {
				case SyncConnected:
					System.out.println("Re-Synchronized to Server");
					// In this particular example we don't need to do anything
					// here - watches are automatically re-registered with
					// server and any watches triggered while the client was
					// disconnected will be delivered (in order of course)
					break;
				case Expired:
					// Game over, man - GAME OVER
					System.out.println("ZK Session expired!");
					dead = true;
					listener.closing(Code.SESSIONEXPIRED.intValue());
					break;
				case Disconnected:
					// We've been disconnected from the server
					// We may be able to recover.
					System.out.println("Partitioned from ZK Ensemble. Awaiting reconnect...");
			}
		} else {
			if (path != null && path.equals(znode)) {
				// Something has changed on the node, let's find out
				System.out.println("Checking if node exists");
				zk.exists(znode, true, this, null);
			}
		}

		// Optionally propagate to other watchers on this process
		if (chainedWatcher != null) {
			chainedWatcher.process(event);
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		System.out.println("Received Stat Event!");
		boolean exists;
		switch (Code.get(rc)) {
			case OK:
				exists = true;
				break;
			case NONODE:
				exists = false;
				break;
			case SESSIONEXPIRED:
			case NOAUTH:
				dead = true;
				listener.closing(rc);
				return;
			case CONNECTIONLOSS:
				System.out.println("Could not reach ZK Ensemble");
				return;
			default:
				// Retry errors
				zk.exists(znode, true, this, null);
				return;
		}

		byte b[] = null;

		if (exists) {
			try {
				b = zk.getData(znode, false, null);
			} catch (KeeperException e) {
				// We don't need to worry about recovering now. The watch
				// callbacks will kick off any exception handling
				e.printStackTrace();
			} catch (InterruptedException e) {
				return;
			}
		}

		// If the data has changed since the last exists call, signal a change to the event listener (Executor)
		if ((b == null && b != prevData) || (b != null && !Arrays.equals(prevData, b)))
		{
			listener.processExistsStatusChangeEvent(b);
			prevData = b;
		}
	}

	public boolean isDead() {
		return dead;
	}
}
