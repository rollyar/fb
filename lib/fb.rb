# frozen_string_literal: true

require "fb/fb_ext"

module Fb
  class Connection
    def execute_script(sql)
      stmts = []
      delim = ";"
      while sql =~ /\S/
        stmt, sql = sql.split(delim, 2)
        if stmt =~ /^\s*set\s+term\s+(\S+)/i
          delim = ::Regexp.last_match(1)
        elsif stmt =~ /\S/
          stmts << stmt
        end
      end
      transaction do
        stmts.each do |stmt|
          execute(stmt)
        end
      end
    end
  end
#   class Fb::Connection
#   def execute_script(sql_schema)
#     transaction do
#       sql_schema.strip.split(';').each do |stmt|
#         execute(stmt.strip) unless stmt.strip.empty?
#       end
#     end
#   end
# end


end
