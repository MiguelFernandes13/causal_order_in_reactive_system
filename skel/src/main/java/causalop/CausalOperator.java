package causalop;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.FlowableOperator;

public class CausalOperator<T> implements FlowableOperator<T, CausalMessage<T>>{
    private final int n;
    private int[] v;
    private Set<CausalMessage<T>> q;
    private Subscription subscription;
    
    public CausalOperator(int n) {
        this.n = n;
        this.v = new int[n];
        for (int i = 0; i < n; i++) {
            v[i] = 0;
        }
        this.q = new TreeSet<>();
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
                    boolean repeat = true;
                    while (repeat){
                        repeat = false;
                        Iterator<CausalMessage<T>> it = q.iterator();
                        while (it.hasNext()){
                            CausalMessage<T> m2 = it.next();
                            if (isCausal(m2)){
                                v[m2.j]++;
                                it.remove();
                                child.onNext(m2.payload);
                                repeat = true;
                                break;
                            }
                        }
                    }
                }
                else if (!isDuplicated(m)){
                    q.add(m);
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
                if (!q.isEmpty())
                    onError(new IllegalArgumentException("Queue is not empty"));
                child.onComplete();
            }
        };
    }

}
