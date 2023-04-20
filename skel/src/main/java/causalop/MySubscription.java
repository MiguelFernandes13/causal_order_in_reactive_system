package causalop;

import java.util.List;
import org.reactivestreams.Subscriber;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscription;

public class MySubscription<T> implements Subscription {
    private Subscriber<? super T> child;
    private AtomicLong childCredits;
    private AtomicLong myCredits;
    private Subscription subscription;
    private List<T> delivered;

    

    public MySubscription(Subscriber<? super T> child, AtomicLong childCredits, AtomicLong myCredits,
            Subscription subscription, List<T> delivered) {
        this.child = child;
        this.childCredits = childCredits;
        this.myCredits = myCredits;
        this.subscription = subscription;
        this.delivered = delivered;
    }

    @Override
    public void request(long n) {
        childCredits.addAndGet(n);
        while(childCredits.get() > 0 && delivered.size() > 0) {
            T payload = delivered.remove(0);
            myCredits.incrementAndGet();
            child.onNext(payload);
            childCredits.decrementAndGet();
            subscription.request(1);
        }
    }

    @Override
    public void cancel() {
        subscription.cancel();
    }
    
}
