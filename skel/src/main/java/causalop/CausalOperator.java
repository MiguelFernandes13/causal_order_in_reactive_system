package causalop;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.FlowableOperator;

import causalop.CausalQueue;

public class CausalOperator<T> implements FlowableOperator<T, CausalMessage<T>>{
    private final int n;
    private int[] v;
    private Set<CausalMessage<T>> q;
    private Subscription subscription;
    private  CausalQueue queues;
    
    public CausalOperator(int n) {
        this.n = n;
        this.v = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = 0;
        }
        this.q = new TreeSet<>();
        this.queues = new CausalQueue(n);
    }

    public boolean isCausal(CausalMessage<T> m) {
        boolean causal = true;
        if (v[m.j] + 1 == m.v[m.j]) {
            for(int i = 0; i < n; i++) {
                if (i != m.j && v[i] < m.v[i]) {
                    causal = false;
                    break;
                }
            }
        }
        else
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

    public boolean sendFromQueue(int j, Subscriber<? super T> child) {
        try {
            CausalMessage<T> message = this.queues.dequeueMessage(j);
            if (isCausal(message)) {
                child.onNext(message.payload);
                this.v[message.j]++;
                return true;
            } else {
                this.queues.addMessage(message);
                return false;
            }
        } catch (NoSuchElementException e) {
            return false;
        }
    }
    public void sendFromQueues(Subscriber<? super T> child) {
        int noCausal = 0;
        for (int i = 0; noCausal < n; i++) {
            int j = i%n;
            while(sendFromQueue(j, child))
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
                child.onSubscribe(parent);
            }

            @Override
            public void onNext(@NonNull CausalMessage<T> m) {     
                if (isCausal(m)){
                    v[m.j]++;
                    child.onNext(m.payload);
                    sendFromQueues(child);
                }
                else if (!isDuplicated(m)){
                    queues.addMessage(m);
                    subscription.request(1);
                }
                else
                    subscription.request(1);
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
