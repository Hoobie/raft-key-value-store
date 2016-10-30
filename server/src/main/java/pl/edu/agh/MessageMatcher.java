package pl.edu.agh;

import rx.Observable;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class MessageMatcher {

    @SafeVarargs
    public static Observable<Void> match(Object object, Function<Object, Optional<Observable<Void>>>... handler) {
        for (Function<Object, Optional<Observable<Void>>> h : handler) {
            Optional<Observable<Void>> optionalObservable = h.apply(object);
            if (optionalObservable.isPresent()) {
                return optionalObservable.get();
            }
        }
        return Observable.empty();
    }

    public static <T> Function<Object, Optional<Observable<Void>>> message(Class<T> type, Function<T, Observable<Void>> handler) {
        return o -> Optional.of(o).filter(type::isInstance).map(type::cast).filter(Objects::isNull).map(handler);
    }
}
