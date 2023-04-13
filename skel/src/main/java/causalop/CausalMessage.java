package causalop;

public class CausalMessage<T> implements Comparable<CausalMessage<T>> {
    int j, v[];
    public T payload;

    public CausalMessage(T payload, int j, int... v) {
        this.payload = payload;
        this.j = j;
        this.v = v;
    }
    
    @Override
    public int compareTo(CausalMessage<T> arg0) {
        if (arg0 instanceof CausalMessage) {
            CausalMessage<T> m = (CausalMessage<T>) arg0;
            if (m.j == j) {
                for (int i = 0; i < v.length; i++) {
                    if (m.v[i] > v[i])
                        return 1;
                    else if (m.v[i] < v[i])
                        return -1;
                }
                return 0;
            }
        }
        return 1;        
    }

    
}
