package causalop;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.FlowableOperator;

public class CausalOperator<T> implements FlowableOperator<T, CausalMessage<T>> {
    private final int n;
    private int[] v;
    private List<T> delivered;
    private Subscription subscription;
    private CausalQueues queues;
    private AtomicLong myCredits;
    private AtomicLong childCredits;
    private ReentrantLock lock;

    public CausalOperator(int n) {
        this.n = n;
        this.v = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = 0;
        }
        this.delivered = new ArrayList<>();
        this.queues = new CausalQueues(n);
        this.myCredits = new AtomicLong(501);
        this.childCredits = new AtomicLong(0);
        this.lock = new ReentrantLock();
    }

    public CausalOperator(int n, long bufferSize) {
        this.n = n;
        this.v = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = 0;
        }
        this.delivered = new ArrayList<>();
        this.queues = new CausalQueues(n);
        // Buffer size is supposed to be the allowed maximum. If an insertion is made
        // that makes the queue size bufferSize + 1
        // then an error is thrown. Thus we give bufferSize + 1 credits to give the
        // opportunity to the parent stream to
        // flush the queue with a new message. If it adds a message to the quarantine
        // then it call onError.
        this.myCredits = new AtomicLong(bufferSize + 1);
        this.childCredits = new AtomicLong(0);
        this.lock = new ReentrantLock();
    }

    public boolean isCausal(CausalMessage<T> m) {
        boolean causal = true;
        if (v[m.j] + 1 == m.v[m.j]) {
            for (int i = 0; i < n; i++) {
                if (i != m.j && v[i] < m.v[i]) {
                    causal = false;
                    break;
                }
            }
        } else
            causal = false;
        return causal;
    }

    public boolean isDuplicated(CausalMessage<T> m) {
        boolean duplicated = false;
        if (v[m.j] + 1 > m.v[m.j]) {
            duplicated = true;
        }
        return duplicated;
    }

    public boolean sendFromQueue(int j) {
        try {
            CausalMessage<T> message = this.queues.dequeueMessage(j);
            if (isCausal(message)) {
                delivered.add(message.payload);
                this.v[message.j]++;
                return true;
            } else {
                this.queues.queueMessage(message);
                return false;
            }
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    public void sendFromQueues() {
        int noCausal = 0;
        for (int i = 0; noCausal < n; i++) {
            int j = i % n;
            while (sendFromQueue(j))
                noCausal = 0;
            noCausal += 1;
        }
    }

    @Override
    public @NonNull Subscriber<? super @NonNull CausalMessage<T>> apply(
            @NonNull Subscriber<? super @NonNull T> child) throws Throwable {

        return new Subscriber<CausalMessage<T>>() {

            @Override
            public void onSubscribe(@NonNull Subscription parent) {
                subscription = parent;
                subscription.request(myCredits.get());
                child.onSubscribe(new MySubscription<>(child, childCredits, myCredits, subscription, delivered, lock));
            }

            @Override
            public void onNext(@NonNull CausalMessage<T> m) {
                try {

                    if (isCausal(m)) {
                        v[m.j]++;
                        delivered.add(m.payload);
                        myCredits.decrementAndGet();
                        sendFromQueues();
                    } else if (!isDuplicated(m)) {
                        queues.queueMessage(m);
                        myCredits.decrementAndGet();
                    } else {
                        subscription.request(1);
                    }

                    try {
                        lock.lock();
                        while (childCredits.get() > 0 && delivered.size() > 0) {
                            T payload = delivered.remove(0);
                            myCredits.incrementAndGet();
                            child.onNext(payload);
                            System.out.println("Delivered from onNext: " + payload);
                            childCredits.decrementAndGet();
                            subscription.request(1);
                        }
                    } finally {
                        lock.unlock();
                    }

                    if (myCredits.get() <= 0 && delivered.size() == 0) {
                        onError(new MessageOverflowException(
                                "Amount of Messages waiting to be delivered and in quarantine" +
                                        " exceeded the maximum amount."));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            @Override
            public void onError(@NonNull Throwable e) {
                child.onError(e);
            }

            @Override
            public void onComplete() {
                if (!queues.allEmpty())
                    onError(new IllegalArgumentException("Queue is not empty"));
                child.onComplete();
            }
        };
    }

}
