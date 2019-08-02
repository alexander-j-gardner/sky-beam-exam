package eventwatcherio.event.validation;

import com.sky.beam.exam.io.eventwatcherio.event.ContentWatchedEvent;
import com.sky.beam.exam.io.eventwatcherio.event.validation.ContentWatchedEventValidator;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.OffsetDateTime;

public class ContentWatchedEventValidatorTest {

    @Test
    public void testNotValidContentWatchedEvent() {
        ContentWatchedEventValidator validator = new ContentWatchedEventValidator(70L, 100L);
        Assert.assertFalse(validator.isValid(ContentWatchedEvent.create(OffsetDateTime.now(), "AlexG", "Blake's Seven", Duration.ofSeconds(10))));
        Assert.assertFalse(validator.isValid(ContentWatchedEvent.create(OffsetDateTime.now(), "AlexG", "Blake's Seven", Duration.ofSeconds(70))));
        Assert.assertFalse(validator.isValid(ContentWatchedEvent.create(OffsetDateTime.now(), "AlexG", "Blake's Seven", Duration.ofSeconds(100))));
    }

    @Test
    public void testValidContentWatchedEvent() {
        ContentWatchedEventValidator validator = new ContentWatchedEventValidator(70L, 100L);
        Assert.assertTrue(validator.isValid(ContentWatchedEvent.create(OffsetDateTime.now(), "AlexG", "Stranger Things", Duration.ofSeconds(71))));
        Assert.assertTrue(validator.isValid(ContentWatchedEvent.create(OffsetDateTime.now(), "AlexG", "Stranger Things", Duration.ofSeconds(99))));
    }
}
