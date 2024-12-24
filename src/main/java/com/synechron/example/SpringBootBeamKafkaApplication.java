package com.synechron.example;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringBootBeamKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootBeamKafkaApplication.class, args);
	}

	@Autowired
	private PipelineOptions pipelineOptions;

	@Bean
	public Pipeline createPipeline() {
		// Initialize Apache Beam pipeline with options
		Pipeline pipeline = Pipeline.create(pipelineOptions);

		// Your existing pipeline configuration remains unchanged...
		// Consume messages, process, and publish as before.

		return pipeline;
	}

	@Bean
	public PipelineOptions pipelineOptions() {
		PipelineOptions options = PipelineOptionsFactory.create();
		options.setRunner(DirectRunner.class); // Use FlinkRunner.class or DataflowRunner.class as needed
		return options;
	}

}
