package causalop;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.ObservableOperator;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.observers.DisposableObserver;

public class CausalOperator<T> implements ObservableOperator<T, CausalMessage<T>> {
    private final int n;
    private int[] v;
    private Set<CausalMessage<T>> q;

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
    public @NonNull Observer<? super CausalMessage<T>> apply(@NonNull Observer<? super T> down) throws Throwable {
        return new DisposableObserver<CausalMessage<T>>() {
            @Override
            public void onNext(@NonNull CausalMessage<T> m) {
                if (isCausal(m)){
                    v[m.j]++;
                    down.onNext(m.payload);
                    boolean repeat = true;
                    while (repeat){
                        repeat = false;
                        Iterator<CausalMessage<T>> it = q.iterator();
                        while (it.hasNext()){
                            CausalMessage<T> m2 = it.next();
                            if (isCausal(m2)){
                                v[m2.j]++;
                                it.remove();
                                down.onNext(m2.payload);
                                repeat = true;
                                break;
                            }
                        }
                    }
                }
                else if (!isDuplicated(m))
                    q.add(m);

            }

            @Override
            public void onError(@NonNull Throwable e) {
                down.onError(e); 
            }

            @Override
            public void onComplete() {
                if (!q.isEmpty())
                    onError(new IllegalArgumentException("Queue is not empty"));
                down.onComplete();
            }
        };
    }
}
