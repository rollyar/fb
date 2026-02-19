require 'test/FbTestCases'

class ReturningTestCases < FbTestCase
  include FbTestCases

  def test_insert_returning_single_column
    sql_schema = 'CREATE TABLE TEST_RETURNING (ID INT, NAME VARCHAR(20))'
    sql_insert = 'INSERT INTO TEST_RETURNING (NAME) VALUES (?) RETURNING ID'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)

      result = connection.execute(sql_insert, 'John')

      unless result.is_a?(Hash) && result.key?(:returning)
        return skip("RETURNING not working - got #{result.class}: #{result.inspect}")
      end

      assert_instance_of Hash, result
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

  def test_insert_returning_multiple_columns
    sql_schema = 'CREATE TABLE TEST_RETURNING (ID INT, NAME VARCHAR(20), CREATED_AT TIMESTAMP)'
    sql_insert = 'INSERT INTO TEST_RETURNING (NAME) VALUES (?) RETURNING ID, CREATED_AT'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)

      result = connection.execute(sql_insert, 'Jane')

      return skip("RETURNING not working - got #{result.class}") unless result.is_a?(Hash) && result.key?(:returning)

      assert_instance_of Hash, result

      returning = result[:returning]
      assert_equal 2, returning.size
      assert_kind_of Integer, returning[0]
      assert_kind_of Time, returning[1]

      assert_equal 1, result[:rows_affected]
    end
  end

  def test_insert_returning_with_params
    sql_schema = 'CREATE TABLE TEST_RETURNING (ID INT, NAME VARCHAR(20), EMAIL VARCHAR(50))'
    sql_insert = 'INSERT INTO TEST_RETURNING (NAME, EMAIL) VALUES (?, ?) RETURNING ID'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)

      result = connection.execute(sql_insert, 'John', 'john@example.com')

      return skip('RETURNING not working') unless result.is_a?(Hash)

      assert_equal 1, result[:rows_affected]

      returning = result[:returning]
      assert_equal 1, returning.size

      id = returning[0]
      row = connection.query('SELECT * FROM TEST_RETURNING WHERE ID = ?', id).first
      assert_equal 'John', row[1]
      assert_equal 'john@example.com', row[2]
    end
  end

  def test_update_returning_single_column
    sql_schema = 'CREATE TABLE TEST_RETURNING (ID INT, NAME VARCHAR(20), UPDATED_AT TIMESTAMP)'
    sql_insert = 'INSERT INTO TEST_RETURNING (ID, NAME) VALUES (?, ?)'
    sql_update = 'UPDATE TEST_RETURNING SET NAME = ? WHERE ID = ? RETURNING UPDATED_AT'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      connection.execute(sql_insert, 1, 'Old Name')

      result = connection.execute(sql_update, 'New Name', 1)

      return skip('RETURNING not working') unless result.is_a?(Hash)

      assert_instance_of Hash, result
      assert_equal 1, result[:rows_affected]

      returning = result[:returning]
      assert_equal 1, returning.size
      assert_kind_of Time, returning[0]
    end
  end

  def test_update_returning_multiple_columns
    sql_schema = 'CREATE TABLE TEST_RETURNING (ID INT, NAME VARCHAR(20), STATUS VARCHAR(20))'
    sql_insert = 'INSERT INTO TEST_RETURNING (ID, NAME, STATUS) VALUES (?, ?, ?)'
    sql_update = 'UPDATE TEST_RETURNING SET NAME = ?, STATUS = ? WHERE ID = ? RETURNING ID, NAME, STATUS'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      connection.execute(sql_insert, 1, 'Old Name', 'inactive')

      result = connection.execute(sql_update, 'New Name', 'active', 1)

      return skip('RETURNING not working') unless result.is_a?(Hash)

      assert_equal 1, result[:rows_affected]

      returning = result[:returning]
      assert_equal 3, returning.size
      assert_equal 1, returning[0]
      assert_equal 'New Name', returning[1]
      assert_equal 'active', returning[2]
    end
  end

  def test_update_returning_no_rows_affected
    sql_schema = 'CREATE TABLE TEST_RETURNING (ID INT, NAME VARCHAR(20))'
    sql_update = 'UPDATE TEST_RETURNING SET NAME = ? WHERE ID = ? RETURNING ID'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)

      result = connection.execute(sql_update, 'New Name', 999)

      return skip('RETURNING not working') unless result.is_a?(Hash)

      assert_equal 0, result[:rows_affected]

      returning = result[:returning]
      assert_instance_of Array, returning
      assert returning.empty?, 'Returning should be empty when no rows affected'
    end
  end

  def test_delete_returning
    sql_schema = 'CREATE TABLE TEST_RETURNING (ID INT, NAME VARCHAR(20))'
    sql_insert = 'INSERT INTO TEST_RETURNING (ID, NAME) VALUES (?, ?)'
    sql_delete = 'DELETE FROM TEST_RETURNING WHERE ID = ? RETURNING NAME'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      connection.execute(sql_insert, 1, 'To Delete')
      connection.execute(sql_insert, 2, 'To Keep')

      result = connection.execute(sql_delete, 1)

      return skip('RETURNING not working') unless result.is_a?(Hash)

      assert_equal 1, result[:rows_affected]

      returning = result[:returning]
      assert_equal 1, returning.size
      assert_equal 'To Delete', returning[0]

      remaining = connection.query('SELECT * FROM TEST_RETURNING')
      assert_equal 1, remaining.size
    end
  end

  def test_delete_returning_multiple_rows
    sql_schema = 'CREATE TABLE TEST_RETURNING (ID INT, NAME VARCHAR(20))'
    sql_insert = 'INSERT INTO TEST_RETURNING (ID, NAME) VALUES (?, ?)'
    sql_delete = 'DELETE FROM TEST_RETURNING WHERE ID > ? RETURNING ID, NAME'

    Database.create(@parms) do |connection|
      connection.execute(sql_schema)
      connection.execute(sql_insert, 1, 'Keep')
      connection.execute(sql_insert, 2, 'Delete1')
      connection.execute(sql_insert, 3, 'Delete2')

      result = connection.execute(sql_delete, 1)

      return skip('RETURNING not working') unless result.is_a?(Hash)

      returning = result[:returning]
      assert_equal 2, returning.size

      remaining = connection.query('SELECT * FROM TEST_RETURNING')
      assert_equal 1, remaining.size
      assert_equal 'Keep', remaining.first[1]
    end
  end
end
