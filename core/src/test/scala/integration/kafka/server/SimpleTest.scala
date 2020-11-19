package kafka.server

import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock

import org.junit.Test
import sun.management.VMManagement


class SimpleTest {

  def getPid(): Integer = {
    val runtime = java.lang.management.ManagementFactory.getRuntimeMXBean
    val jvm = runtime.getClass.getDeclaredField("jvm")
    jvm.setAccessible(true)
    val mgmt = jvm.get(runtime).asInstanceOf[VMManagement]
    val pid_method = mgmt.getClass.getDeclaredMethod("getProcessId")
    pid_method.setAccessible(true)

    val pid = pid_method.invoke(mgmt).asInstanceOf[Integer]
    pid
  }

  @Test
  def simpleTest(): Unit = {
    val pid = getPid()
    println(s"pid is $pid")

    val lock1 = new ReentrantLock()
    val lock2 = new ReentrantLock()

    val r1 = new Runnable {
      override def run(): Unit = {
        lock1.lock()
        println("got lock1")
        try {
          Thread.sleep(5000)
          lock2.lock()
          lock2.unlock()
        } finally {
          lock1.unlock()
        }
      }
    }

    val r2 = new Runnable {
      override def run(): Unit = {
        lock2.lock()
        println("got lock2")
        try {
          Thread.sleep(5000)
          lock1.lock()
          lock1.unlock()
        } finally {
          lock2.unlock()
        }
      }
    }

    //val service = Executors.newFixedThreadPool(2);
    val t1 = new Thread(r1)
    t1.start()
    val t2 = new Thread(r2)
    t2.start()

    t1.join()
    t2.join()
    println("all threads have finished")
  }
}
