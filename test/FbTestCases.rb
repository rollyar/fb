require 'fileutils'
require 'tmpdir'
include FileUtils

require 'fb'

if RUBY_VERSION.match?(/^2/)
  require 'minitest/autorun'

  Minitest::Test = MiniTest::Unit::TestCase unless Minitest.const_defined?('Test')

  class FbTestCase < Minitest::Test
  end

else
  require 'test/unit'

  class FbTestCase < Test::Unit::TestCase
    def default_test; end
  end
end

module FbTestCases
  include Fb

  def remote_db?
    @db_file.to_s.start_with?('/firebird/') || ENV['FIREBIRD_DATA_DIR']
  end

  def setup
    data_dir = ENV['FIREBIRD_DATA_DIR'] || case RUBY_PLATFORM
                                           when /win32/ then 'c:/var/fbdata'
                                           when /darwin/ then __dir__
                                           else '/tmp'
                                           end
    test_name = "#{$$}_#{@name || 'test'}"
    @db_file = File.join(data_dir, "drivertest_#{test_name}.fdb")
    @db_host = ENV['FIREBIRD_HOST'] || 'localhost'
    @username = ENV['FIREBIRD_USER'] || 'sysdba'
    @password = ENV['FIREBIRD_PASSWORD'] || 'masterkey'
    @parms = {
      database: "#{@db_host}:#{@db_file}",
      username: @username,
      password: @password,
      charset: 'UTF8',
      role: 'READER'
    }
    @parms_s = "database = #{@db_host}:#{@db_file}; username = #{@username}; password = #{@password}; charset = UTF8; role = READER;"
    @fb_version = -1

    begin
      db = Fb::Database.new(@parms)
      begin
        db.drop
      rescue StandardError
        nil
      end
    rescue Fb::Error
      # Database might not exist, that's OK
    rescue StandardError
      # Ignore other errors during cleanup
    end

    rm_rf @db_file
    rm_rf "#{@db_file}.fdb"

    db = Database.create(@parms)
    connection = db.connect
    d = connection.query("SELECT substring(rdb$get_context('SYSTEM', 'ENGINE_VERSION') from 1 for 1) from rdb$database")
    @fb_version = Integer(d.first[0])
    connection.close
    begin
      db.drop
    rescue StandardError
      nil
    end

    rm_rf @db_file
  end

  def expected_columns
    case @fb_version
    when 5, 4
      6
    when 3
      5
    else
      4
    end
  end
end

class Fb::Connection
  def execute_script(sql_schema)
    transaction do
      sql_schema.strip.split(';').each do |stmt|
        execute(stmt)
      end
    end
  end
end
