$: << File.dirname(__FILE__)
$: << File.join(File.dirname(__FILE__), '..')

require 'DatabaseTestCases'
require 'ConnectionTestCases'
require 'CursorTestCases'
require 'DataTypesTestCases'
require 'NumericDataTypesTestCases'
require 'TransactionTestCases'
# require 'ReturningTestCases'  # Disabled - RETURNING feature under development
require 'EncodingTestCases' if RUBY_VERSION.match?(/^1.9/)
require 'bigdecimal'
