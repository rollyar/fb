require 'test/FbTestCases'

class ReturningTestCases < FbTestCase
  include FbTestCases

  def test_insert_returning_single_column
    sql_schema = 'CREATE TABLE TEST_RETURNING (ID INT, NAME VARCHAR(20))'
    sql_insert = 'INSERT INTO TEST_RETURNING (NAME) VALUES (?) RETURNING ID'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)

      result = connection.execute(sql_insert, 'John')

      assert_instance_of Hash, result, "RETURNING not working - got #{result.class}"
      assert result.key?(:returning), 'Result should have :returning key'
      assert result.key?(:rows_affected), 'Result should have :rows_affected key'

      returning = result[:returning]
      assert_instance_of Array, returning
      assert_equal 1, returning.size
      assert_kind_of Integer, returning[0]

      assert_equal 1, result[:rows_affected]

      row = connection.query('SELECT * FROM TEST_RETURNING').first
      assert_equal 'John', row[1]
    end
  end

  def test_insert_returning_with_params
    sql_schema = 'CREATE TABLE TEST_RETURNING (ID INT, NAME VARCHAR(20), EMAIL VARCHAR(50))'
    sql_insert = 'INSERT INTO TEST_RETURNING (NAME, EMAIL) VALUES (?, ?) RETURNING ID'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)

      result = connection.execute(sql_insert, 'John', 'john@example.com')

      assert_instance_of Hash, result, "RETURNING not working - got #{result.class}"

      assert_equal 1, result[:rows_affected]

      returning = result[:returning]
      assert_equal 1, returning.size

      id = returning[0]
      row = connection.query('SELECT * FROM TEST_RETURNING WHERE ID = ?', id).first
      assert_equal 'John', row[1]
      assert_equal 'john@example.com', row[2]
    end
  end
end
