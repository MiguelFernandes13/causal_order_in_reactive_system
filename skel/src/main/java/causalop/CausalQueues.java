package causalop;

import java.util.*;

public class CausalQueues<T> {
    private List<Set<CausalMessage<T>>> queues;
    public CausalQueues(int n) {
        this.queues = new ArrayList<>(n);
        for (int i = 0; i < n; i++)
            this.queues.add(new TreeSet<>());
    }

    public void queueMessage(CausalMessage<T> message) {
        Set<CausalMessage<T>> q;
        q = this.queues.get(message.j);
        q.add(message);
    }

    public CausalMessage<T> dequeueMessage(int j) throws NoSuchElementException {
        CausalMessage<T> res;
        Set<CausalMessage<T>> q;
        try {
            if ( (q = this.queues.get(j)) != null ) {
                if (q.size() > 0) {
                    res = q.stream().findFirst().get();
                    q.remove(res);
                    return res;
                }
            }
        } catch (IndexOutOfBoundsException e) {
        }
        throw new NoSuchElementException();
    }

    public boolean allEmpty() {
        for (Set<CausalMessage<T>> s : this.queues)
            if (!s.isEmpty()) return false;
        return true;
    }


}
