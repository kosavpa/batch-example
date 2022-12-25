package owl.home.configs;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ResourceLoader;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionManager;
import owl.home.entity.Singer;
import owl.home.util.StepExecutorListener;

import javax.sql.DataSource;


@Configuration
@EnableBatchProcessing
@ComponentScan("owl.home")
@Import(DataSourceConfiguration.class)
public class BatchConfiguration {
    @Autowired
    private DataSource dataSource;
    @Autowired
    private ResourceLoader resourceLoader;
    @Autowired
    private StepExecutorListener stepExecutionListener;
    @Autowired
    private PlatformTransactionManager transactionManager;

    @Bean
    @Qualifier("step1")
    public Job job(Step step1, JobRepository jobRepository){

        return new JobBuilder("SINGER_JOB", jobRepository)
                    .start(step1)
                    .build();
    }

    @Bean
    public Step step1(ItemReader<Singer> itemReader, ItemProcessor<Singer, Singer> itemProcessor, ItemWriter<Singer> itemWriter, JobRepository jobRepository){

        return new StepBuilder("STEP1", jobRepository)
                    .listener(stepExecutionListener)
                    .<Singer, Singer>chunk(1, transactionManager)
                    .reader(itemReader)
                    .processor(itemProcessor)
                    .writer(itemWriter)
                    .build();
    }

    @Bean
    public ItemReader itemReader(){
        BeanWrapperFieldSetMapper<Singer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Singer.class);

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setNames("firstName", "lastName", "song");

        DefaultLineMapper lineMapper = new DefaultLineMapper();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        FlatFileItemReader<Singer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(resourceLoader.getResource("classpath:/owl/home/util/test-data.csv"));
        itemReader.setLineMapper(lineMapper);

        return itemReader;
    }

    @Bean
    public ItemWriter itemWriter(){
        JdbcBatchItemWriter<Singer> itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Singer>());
        itemWriter.setSql("INSERT INTO singer(first_name, last_name, song) values(:firstName, :lastName, :song)");
        itemWriter.setDataSource(dataSource);

        return itemWriter;
    }
}
