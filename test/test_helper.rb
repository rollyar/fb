require 'rubygems'
require 'tmpdir'
require 'fileutils'
require 'securerandom'
require 'bigdecimal'
require 'fb'
require 'minitest/autorun'

class FbTestCase < Minitest::Test
  include Fb

  def setup
    @db_host = ENV.fetch('DB_HOST') { '0.0.0.0' }
    @parms = get_db_conn_params('drivertest.fdb')
    @parms_s = get_db_conn_string(@parms)
    @db_file = @parms[:database].split(':', 2).last
    @fb_version = get_fb_version
  end

  def teardown
    cleanup_database(@parms)
  rescue Fb::Error => e
    puts "ADVERTENCIA en limpieza: #{e.message}"
  end

  def get_db_conn_params(dbname = nil)
    dbname ||= "drivertest.#{Process.pid}.#{SecureRandom.hex(8)}.fdb"

    db_file = case RUBY_PLATFORM
              when /win32/
                File.join('c:', 'var', 'fbdata', dbname)
              else
                File.join('/', 'tmp', dbname)
              end
    {
      database: "#{@db_host}:#{db_file}",
      username: 'sysdba',
      password: 'masterkey',
      charset: 'NONE',
      role: 'READER'
    }
  end

  def get_db_conn_string(params = nil)
    params ||= get_db_conn_params
    "database = #{params[:database]}; username = #{params[:username]}; password = #{params[:password]}; charset = NONE; role = READER;"
  end

  def get_fb_version
    @fb_version ||= begin
      version = 3 # Valor por defecto seguro
      params = get_db_conn_params("fbversion.#{Process.pid}.#{Thread.current.object_id}.fdb")

      max_attempts = 2
      attempts = 0

      while attempts < max_attempts
        begin
          db_path = params[:database].split(':', 2).last
          File.delete(db_path) if File.exist?(db_path)

          Database.create(params) do |connection|
            d = connection.query("SELECT substring(rdb$get_context('SYSTEM', 'ENGINE_VERSION') from 1 for 1) from rdb$database")
            version = Integer(d.first[0])
            connection.drop
          end
          break
        rescue Fb::Error => e
          attempts += 1
          if attempts >= max_attempts
            puts "ADVERTENCIA: No se pudo determinar versión Firebird, usando 3 por defecto: #{e.message}"
          else
            sleep 0.5
          end
        ensure
          cleanup_database(params)
        end
      end

      version
    end
  end

  def cleanup_database(params)
    return unless params

    db_path = params[:database].split(':', 2).last

    begin
      Database.drop(params)
    rescue Fb::Error
      # Ignorar errores de drop
    end

    # Eliminación agresiva del archivo
    max_attempts = 3
    attempts = 0

    while attempts < max_attempts && File.exist?(db_path)
      begin
        File.delete(db_path)
        break
      rescue Errno::EACCES, Errno::EBUSY
        attempts += 1
        sleep 0.2 * attempts
      rescue Errno::ENOENT
        break
      end
    end
  end

  def assert_response_size_of_version(response_size)
    case @fb_version
    when 5
      assert_equal 6, response_size
    when 4
      assert_equal 6, response_size
    when 3
      assert_equal 5, response_size
    else
      assert_equal 4, response_size
    end
  end

  def with_database(&block)
    max_attempts = 2
    attempts = 0

    while attempts < max_attempts
      begin
        Database.create(@parms, &block)
        break
      rescue Fb::Error => e
        attempts += 1
        raise e if attempts >= max_attempts

        cleanup_database(@parms)
        sleep 0.5
      end
    end
  end
end

class Fb::Connection
  def execute_script(sql_schema)
    transaction do
      sql_schema.strip.split(';').each do |stmt|
        execute(stmt.strip) unless stmt.strip.empty?
      end
    end
  end
end
