require 'test/unit'
require 'test/unit/FbTestCases'
require 'fb.so'
require 'fileutils'
include Fb
include FileUtils

class CursorTestCases < Test::Unit::TestCase
  include FbTestCases

  def test_fetch_array
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        assert_instance_of Cursor, cursor
        row = cursor.fetch :array
        assert_instance_of Array, row
        assert_equal 4, row.size
      end
      connection.drop
    end
  end
  
  def test_fetch_hash
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        assert_instance_of Cursor, cursor
        row = cursor.fetch :hash
        assert_instance_of Hash, row
        assert_equal 4, row.size
      end
      connection.drop
    end
  end
  
  def test_fetch_all_array
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        assert_instance_of Cursor, cursor
        rows = cursor.fetchall :array
        assert_instance_of Array, rows
        assert_equal 1, rows.size
        assert_instance_of Array, rows[0]
        assert_equal 4, rows[0].size
      end
      connection.drop
    end
  end
  
  def test_fetch_all_hash
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        assert_instance_of Cursor, cursor
        rows = cursor.fetchall :hash
        assert_instance_of Array, rows
        assert_equal 1, rows.size
        assert_instance_of Hash, rows[0]
        assert_equal 4, rows[0].size
      end
      connection.drop
    end
  end
  
  def test_fields_array
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        fields = cursor.fields
        fields_ary = cursor.fields :array
        assert_equal fields, fields_ary
        assert_equal 4, fields.size
        assert_equal "RDB$DESCRIPTION", fields[0].name;
        assert_equal "RDB$RELATION_ID", fields[1].name;
        assert_equal "RDB$SECURITY_CLASS", fields[2].name;
        assert_equal "RDB$CHARACTER_SET_NAME", fields[3].name;
      end
      connection.drop
    end
  end
  
  def test_fields_array_downcased
    Database.create(@parms.merge(:downcase_column_names => true)) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        fields = cursor.fields
        fields_ary = cursor.fields :array
        assert_equal fields, fields_ary
        assert_equal 4, fields.size
        assert_equal "rdb$description", fields[0].name;
        assert_equal "rdb$relation_id", fields[1].name;
        assert_equal "rdb$security_class", fields[2].name;
        assert_equal "rdb$character_set_name", fields[3].name;
      end
      connection.drop
    end
  end
  
  def test_fields_hash
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        fields = cursor.fields :hash
        assert_equal 4, fields.size
        assert_equal 520, fields["RDB$DESCRIPTION"].type_code
        assert_equal 500, fields["RDB$RELATION_ID"].type_code
        assert_equal 452, fields["RDB$SECURITY_CLASS"].type_code
        assert_equal 452, fields["RDB$CHARACTER_SET_NAME"].type_code
      end
      connection.drop
    end
  end
  
  def test_fields_hash_downcased
    Database.create(@parms.merge(:downcase_column_names => true)) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        fields = cursor.fields :hash
        assert_equal 4, fields.size
        assert_equal 520, fields["rdb$description"].type_code
        assert_equal 500, fields["rdb$relation_id"].type_code
        assert_equal 452, fields["rdb$security_class"].type_code
        assert_equal 452, fields["rdb$character_set_name"].type_code
      end
      connection.drop
    end
  end
  
  def test_each_array
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        count = 0
        cursor.each :array do |row|
          count += 1
          assert_instance_of Array, row
          assert_equal 4, row.size
        end
        assert_equal 1, count
      end
      connection.drop
    end
  end
  
  def test_each_hash
    Database.create(@parms) do |connection|
      connection.execute("select * from rdb$database") do |cursor|
        count = 0
        cursor.each :hash do |row|
          count += 1
          assert_instance_of Hash, row
          assert_equal 4, row.size
        end
        assert_equal 1, count
      end
      connection.drop
    end
  end
  
  def test_fetch_after_nil
    Database.create(@parms) do |connection|
      connection.execute("create generator test_seq");
      connection.commit
      connection.execute("select gen_id(test_seq, 1) from rdb$database") do |cursor|
        r1 = cursor.fetch
        assert_not_nil r1
        r2 = cursor.fetch
        assert_nil r2
        assert_raise Error do
          r3 = cursor.fetch
        end
      end
      connection.execute("select * from rdb$database") do |cursor|
        r1 = cursor.fetch
        assert_not_nil r1
        r2 = cursor.fetch
        assert_nil r2
        assert_raise Error do
          r3 = cursor.fetch
        end
      end
      connection.drop
    end
  end
  
  def test_fetch_hash_with_aliased_fields
    sql = "SELECT RDB$DESCRIPTION DES, RDB$RELATION_ID REL, RDB$SECURITY_CLASS SEC, RDB$CHARACTER_SET_NAME FROM RDB$DATABASE"
    Database.create(@parms) do |connection|
      connection.execute(sql) do |cursor|
        assert_instance_of Cursor, cursor
        row = cursor.fetch :hash
        assert_instance_of Hash, row
        assert_equal 4, row.size
        assert row.keys.include?("DES"), "No field DES"
        assert row.keys.include?("REL"), "No field REL"
        assert row.keys.include?("SEC"), "No field SEC"
        assert row.keys.include?("RDB$CHARACTER_SET_NAME"), "No field RDB$CHARACTER_SET_NAME"
      end
      connection.drop
    end
  end
end