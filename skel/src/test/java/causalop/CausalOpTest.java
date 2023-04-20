package causalop;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class CausalOpTest {
    @Test
    public void testOk() {
        var l = Flowable.just(
                new CausalMessage<String>("a", 1, 0, 1),
                new CausalMessage<String>("b", 0, 1, 0),
                new CausalMessage<String>("c", 1, 1, 2))
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[] { "a", "b", "c" });
    }

    @Test
    public void testReorder() {
        var l = Flowable.just(
                new CausalMessage<String>("c", 1, 1, 2),
                new CausalMessage<String>("a", 1, 0, 1),
                new CausalMessage<String>("b", 0, 1, 0))
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[] { "a", "b", "c" });
    }

    @Test
    public void testDupl() {
        var l = Flowable.just(
                new CausalMessage<String>("a", 1, 0, 1),
                new CausalMessage<String>("b", 0, 1, 0),
                new CausalMessage<String>("a", 1, 0, 1),
                new CausalMessage<String>("c", 1, 1, 2))
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[] { "a", "b", "c" });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGap() {
        var l = Flowable.just(
                new CausalMessage<String>("c", 1, 1, 2),
                new CausalMessage<String>("a", 1, 0, 1))
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();
    }

    @Test
    public void testReorder2() {
        var l = Flowable.just(
                new CausalMessage<String>("c", 1, 1, 2),
                new CausalMessage<String>("d", 0, 2, 2),
                new CausalMessage<String>("a", 1, 0, 1),
                new CausalMessage<String>("b", 0, 1, 0))
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[] { "a", "b", "c", "d" });
    }

    @Test
    public void lotMessages() {
        var l = Flowable.interval(10, TimeUnit.MILLISECONDS)
                .take(1001)
                .map(i -> {
                    System.out.println("Emmiting " + i);
                    if (i % 2 == 0)
                        return new CausalMessage<String>("a" + Math.toIntExact(i) / 2, 1, 0, Math.toIntExact(i) / 2);
                    else {
                        int x = 500 + (Math.toIntExact(i) - 1) / 2 + 1;
                        return new CausalMessage<String>("b" + x, 1, 0, x);
                    }

                })
                .onBackpressureBuffer()
                .observeOn(Schedulers.computation())
                .map(i -> {
                    Thread.sleep(50);
                    System.out.println("Received " + Arrays.toString(i.v));
                    return i;
                })
                .lift(new CausalOperator<String>(2, 500))
                .map(i -> {
                    System.out.println("Delivered " + i);
                    return i;
                })
                .toList().blockingGet();

        System.out.println(l.size());
        Assert.assertEquals(l.size(), 1000);
    }

    @Test(expected = causalop.MessageOverflowException.class)
    public void lotMessagesBufferOverflow() {
        var l = Flowable.interval(10, TimeUnit.MILLISECONDS)
                .take(1001)
                .map(i -> {
                    System.out.println("Emmiting " + i);
                    if (i % 2 == 0)
                        return new CausalMessage<String>("a" + Math.toIntExact(i) / 2, 1, 0, Math.toIntExact(i) / 2);
                    else {
                        int x = 500 + (Math.toIntExact(i) - 1) / 2 + 1;
                        return new CausalMessage<String>("b" + x, 1, 0, x);
                    }

                })
                .onBackpressureBuffer()
                .observeOn(Schedulers.computation())
                .map(i -> {
                    Thread.sleep(50);
                    System.out.println("Received " + Arrays.toString(i.v));
                    return i;
                })
                // 500 messages expected to be in the quarantine buffer
                .lift(new CausalOperator<String>(2, 499))
                .map(i -> {
                    System.out.println("Delivered " + i);
                    return i;
                })
                .toList().blockingGet();

    }

    @Test
    public void backPressureTest() throws InterruptedException {
        var l = Flowable.interval(10, TimeUnit.MILLISECONDS)
                .take(501)
                .map(i -> {
                    System.out.println("Emmiting " + i);
                    return new CausalMessage<String>("a" + i, 1, 0, Math.toIntExact(i));
                })
                .lift(new CausalOperator<String>(2, 500))
                .onBackpressureBuffer(500)
                .observeOn(Schedulers.io())
                .flatMap(i -> {
                    return Flowable.just(i)
                            .doOnRequest(n -> {
                                System.out.println("Requesting ");
                            })
                            .delay(15, TimeUnit.MILLISECONDS);
                }, 1)
                .map(i -> {
                    System.out.println("Delivered " + i);
                    return i;
                }).toList().blockingGet();
        Thread.sleep(5000);

        Assert.assertEquals(l.size(), 500);

    }
}
