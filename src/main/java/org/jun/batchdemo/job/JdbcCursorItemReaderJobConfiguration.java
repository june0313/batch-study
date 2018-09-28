package org.jun.batchdemo.job;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jun.batchdemo.domain.Pay;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;

@Slf4j
@Configuration
@AllArgsConstructor
public class JdbcCursorItemReaderJobConfiguration {
	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final DataSource dataSource; // DataSource DI

	private static final int CHUNK_SIZE = 10;

	@Bean
	public Job jdbcCursorItemReaderJob() {
		return jobBuilderFactory.get("jdbcCursorItemReaderJob")
				.start(jdbcCursorItemReaderStep())
				.build();
	}

	@Bean
	public Step jdbcCursorItemReaderStep() {
		return stepBuilderFactory.get("jdbcCursorItemReaderStep")
				.<Pay, Pay>chunk(CHUNK_SIZE)
				.reader(jdbcCursorItemReader())
				.writer(jdbcCursorItemWriter())
				.build();
	}

	@Bean
	public JdbcCursorItemReader<Pay> jdbcCursorItemReader() {
		return new JdbcCursorItemReaderBuilder<Pay>()
				.fetchSize(CHUNK_SIZE)
				.dataSource(dataSource)
				.rowMapper(new BeanPropertyRowMapper<>(Pay.class))
				.sql("SELECT id, amount, tx_name, tx_date_time FROM pay")
				.name("jdbcCursorItemReader")
				.build();
	}

	private ItemWriter<Pay> jdbcCursorItemWriter() {
		return list -> list.forEach(pay -> log.info("Current Pay={}", pay));
	}
}
