module RS2BQ
  class BigQueryTableCreator
    def initialize(big_query_dataset, schema_builder)
      @big_query_dataset = big_query_dataset
      @schema_builder = schema_builder
    end

    def create_table_like(big_query_table_name, redshift_table_name)
      schema = @schema_builder.big_query_schema_from(redshift_table_name)
      @big_query_dataset.create_table(big_query_table_name, schema: schema)
    end
  end
end
