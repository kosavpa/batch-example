package owl.home.processor;


import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;
import owl.home.entity.Singer;


@Component
public class SingerProcessor implements ItemProcessor<Singer, Singer> {
    @Override
    public Singer process(Singer singer) throws Exception {
        String firstName = singer.getFirstName().toUpperCase();
        String lastName = singer.getLastName().toUpperCase();
        String song = singer.getSong().toUpperCase();

        Singer transformSinger = new Singer();
        transformSinger.setFirstName(firstName);
        transformSinger.setLastName(lastName);
        transformSinger.setSong(song);

        return singer;
    }
}
