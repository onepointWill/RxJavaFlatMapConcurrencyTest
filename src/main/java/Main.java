import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.RxHelper;

import java.util.concurrent.TimeUnit;

public class Main extends AbstractVerticle {

    @Override
    public void start() throws Exception {
        super.start();

        Scheduler scheduler = RxHelper.scheduler(vertx);
        Flowable.range(1, 5_000)
                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .map(i -> i * 100)
                .map(i -> i / 100)
                .map(i -> i + 1)
                .map(i -> i - 1)
                .window(80)
                .concatMapCompletable(requests -> {
                    return requests.flatMapCompletable(request -> {
                        return parseRequest(request)
                                .flatMapCompletable(data -> {
                                    return finishedJob(data);
                                })
                                .andThen(finishedRequest(request));
                    });
                })
                .subscribe(this::finishedAll);
    }

    private Flowable<String> parseRequest(int i) {
        System.out.println(String.format("parse request %d", i));
        return Flowable.range(0, i)
                .map(d -> String.format("value:%d", i))
                .flatMap(this::processJob);
    }

    private Flowable<String> processJob(String job) {
        return Flowable.range(0, job.length())
                .map(job::charAt)
                .reduce(Flowable.just("JOB === "), (flowable, character) -> flowable.flatMap(word -> filterJob(word, character)))
                .toFlowable()
                .flatMap(v -> v);
    }

    private Flowable<String> filterJob(String word, Character character) {
        return Flowable.just(word)
                .delay(random(1, 1000), TimeUnit.MILLISECONDS)
                .map(w -> w + character);
    }

    private void finishedAll() {
        System.out.println("=============");
        System.out.println("=============");
        System.out.println("Finished All");
        System.out.println("=============");
        System.out.println("=============");
    }

    private Completable finishedRequest(Integer request) {
        System.out.println(String.format("Request finished: %d", request));
        return Completable.fromAction(() -> {});
    }

    private Completable finishedJob(String job) {
        System.out.println(String.format("Job finished: %s", job));
        return Completable.fromAction(() -> {});
    }

    private Completable finishedJobWithError(Integer request) {
        System.out.println(String.format("Request finished: %d", request));
        return Completable.error(new Throwable("bob"));
    }

    public static void main(String... args) {
        DeploymentOptions deploymentOptions = new DeploymentOptions();
        VertxOptions vertxOptions = new VertxOptions();

        io.vertx.reactivex.core.Vertx vertx = io.vertx.reactivex.core.Vertx.vertx(vertxOptions);
        vertx.rxDeployVerticle(Main.class.getName(), deploymentOptions).subscribe();
    }

    private static long random(double min, double max){
        return ((long) ((Math.random()*((max-min)+1))+min)) ;
    }
}
