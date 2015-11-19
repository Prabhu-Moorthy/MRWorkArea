package thread;

public class thread1 {

	public static void main(String[] args) {
		Runnable r1 = new Runnable() {
			
			@Override
			public void run() {
				System.out.println("Thread 1");
			}
		};
		Thread t1 = new Thread(r1);
		t1.start();
	}

}
