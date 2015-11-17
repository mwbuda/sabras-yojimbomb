
require './base'
require 'yojimbomb/persistence/rdbms'
require 'mysql2'

#un/comment to debug SQL statements
#class Mysql2::Client	
#	def query(sql, options = {})
#		puts "\n!!!SQL: #{sql}"
#		@query_options.each {|opt,v| puts "\tQOPT #{opt} = #{v}"} unless @query_options.nil?
#		options.each {|opt,v| puts "\tXOPT #{opt} = #{v}"} 
#		puts "\n"
#		_query(sql, @query_options.merge(options))
#	end	
#end


#$DBConnect = $*[0]
$DBConnect = {
	:adapter => 'mysql2',
	:host => 'localhost',
	:database => 'yojimbomb_test',
	:user => 'ybtest',
	:password => 'password',	
}

metricsKeeper = Yojimbomb::RdbmsMetricsKeeper.connect($DBConnect)
metricsKeeper.logger = Logger.new(STDOUT)
Yojimbomb::Test.testPersistence(metricsKeeper)
