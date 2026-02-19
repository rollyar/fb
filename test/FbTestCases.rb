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

  def setup
    @db_file = case RUBY_PLATFORM
               when /win32/ then 'c:/var/fbdata/drivertest.fdb'
               when /darwin/ then File.join(__dir__, 'drivertest.fdb')
               when /linux/ then File.join(Dir.tmpdir, 'drivertest.fdb')
               else '/var/fbdata/drivertest.fdb'
               end
    @db_host = ENV['FIREBIRD_HOST'] || 'localhost'
    @username = ENV['FIREBIRD_USER'] || 'sysdba'
    @password = ENV['FIREBIRD_PASSWORD'] || 'masterkey'
    @parms = {
      database: "#{@db_host}:#{@db_file}",
      username: @username,
      password: @password,
      charset: 'NONE',
      role: 'READER'
    }
    @parms_s = "database = #{@db_host}:#{@db_file}; username = #{@username}; password = #{@password}; charset = NONE; role = READER;"
    @fb_version = -1
    rm_rf @db_file
    rm_rf "#{@db_file}.fdb"

    Database.create(@parms) do |connection|
      d = connection.query("SELECT substring(rdb$get_context('SYSTEM', 'ENGINE_VERSION') from 1 for 1) from rdb$database")
      @fb_version = Integer(d.first[0])
      connection.drop
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
