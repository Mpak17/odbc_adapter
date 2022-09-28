module ODBCAdapter
  module Adapters
    # A default adapter used for databases that are no explicitly listed in the
    # registry. This allows for minimal support for DBMSs for which we don't
    # have an explicit adapter.
    class NullODBCAdapter < ActiveRecord::ConnectionAdapters::ODBCAdapter
      VARIANT_TYPE = 'VARIANT'.freeze
      DATE_TYPE = 'DATE'.freeze
      JSON_TYPE = 'JSON'.freeze
      TIMESTAMP = 'TIMESTAMP'.freeze
      # Using a BindVisitor so that the SQL string gets substituted before it is
      # sent to the DBMS (to attempt to get as much coverage as possible for
      # DBMSs we don't support).
      def arel_visitor
        ::ODBCAdapter::SnowflakeInsertManager.new(self)
      end

      # Explicitly turning off prepared_statements in the null adapter because
      # there isn't really a standard on which substitution character to use.
      def prepared_statements
        false
      end

      # Turning off support for migrations because there is no information to
      # go off of for what syntax the DBMS will expect.
      def supports_migrations?
        false
      end
    end

    class SnowflakeInsertManager < Arel::Visitors::ToSql
      private

      def visit_Arel_Nodes_ValuesList(o, collector)
        collector << "VALUES "

        o.rows.each_with_index do |row, i|
          collector << ", " unless i == 0
          collector << "("
          row.each_with_index do |value, k|
            collector << ", " unless k == 0
            case value
            when Nodes::SqlLiteral, Nodes::BindParam
              collector = visit(value, collector)
            else
              collector << quote(value).to_s
            end
          end
          collector << ")"
        end
        collector
      end
    end
  end
end
