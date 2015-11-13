
require './base'
require 'yojimbomb/persistence/rdbms'
require 'mysql2'

class Mysql2::Client
	
	def query(sql, options = {})
		puts "!!!SQL: #{sql}"
		@query_options.each {|opt,v| puts "!!!QOPT #{opt} = v"} unless @query_options.nil?
		options.each {|opt,v| puts "!!!XOPT #{opt} = #{v}"} 
		_query(sql, @query_options.merge(options))
	end
	
end


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
