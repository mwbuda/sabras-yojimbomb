
require 'yojimbomb'
require 'yojimbomb/persistence/rdbms/sequel'

module Yojimbomb
	module RDBMS
		MetricsKeeper = Yojimbomb::RDBMS::SequelMetricsKeeper
	end 
	
	RdbmsMetricsKeeper = Yojimbomb::RDBMS::MetricsKeeper
end

