package causalop;

import java.util.List;
import org.reactivestreams.Subscriber;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.reactivestreams.Subscription;

public class MySubscription<T> implements Subscription {
    private Subscriber<? super T> child;
    private AtomicLong childCredits;
    private AtomicLong myCredits;
    private Subscription subscription;
    private List<T> delivered;
    private ReentrantLock lock;

    

    public MySubscription(Subscriber<? super T> child, AtomicLong childCredits, AtomicLong myCredits,
            Subscription subscription, List<T> delivered, ReentrantLock lock) {
        this.child = child;
        this.childCredits = childCredits;
        this.myCredits = myCredits;
        this.subscription = subscription;
        this.delivered = delivered;
        this.lock = lock;
    }

    @Override
    public void request(long n) {
        try{
            lock.lock();
            childCredits.addAndGet(n);
            while(childCredits.get() > 0 && delivered.size() > 0) {
                T payload = delivered.remove(0);
                myCredits.incrementAndGet();
                child.onNext(payload);
                System.out.println("Delivered from request: " + payload);
                System.out.println("Delivered size: " + delivered.size());
                childCredits.decrementAndGet();
                subscription.request(1);
            }
        } 
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
        }
            lock.unlock();
        }

    @Override
    public void cancel() {
        subscription.cancel();
    }
    
}
