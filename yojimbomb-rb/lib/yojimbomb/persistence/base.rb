
require 'logger'

module Yojimbomb
	
	class MetricsKeeper
		DefaultLoggerOut = StringIO.new
		DefaultLogger = Logger.new(Yojimbomb::MetricsKeeper::DefaultLoggerOut)
		
		attr_writer :logger
		
		def logger()
			@logger.nil? ? @nilLogger : @logger
		end
		
		def self.checkConfiguredLogic(*exps, &block)
			pass = exps.include?( block.arity) unless block.nil?
			raise :invalidMetricKeeperLogic unless pass
			self
		end
		
		{
			:ensureMetricClass => [0,1],
			:ensureMetricType => [1,2],
			:store => [-2,-3],
			:getMetricTypes => [0,1],
			:find => [2,3],
			:remove => [-2,-3]
		}.each do |logic, expArity| 
			eigenclass = class << self ; self ; end
			lname = logic.to_s.strip
			clname = lname[0].upcase + lname[1..-1]
			
			eigenclass.instance_eval do 
				define_method("define#{clname}".to_sym) do |metricClass, &block| 
					self.checkConfiguredLogic(*expArity, &block)
					@logic = Hash.new {|h,k| h[k] = Hash.new } if @logic.nil?
					@logic[logic][metricClass] = block
					self
				end
				
				define_method("#{lname}Logic") do |metricClass|
					@logic = Hash.new {|h,k| h[k] = Hash.new } if @logic.nil?
					@logic[logic][metricClass]
				end
				
			end
			
			define_method("define#{clname}".to_sym) do |metricClass, &block|
				self.class.checkConfiguredLogic(*expArity, &block)
				@logic[logic][metricClass] = block
				self
			end
			
			define_method("hasDefined#{clname}?".to_sym) do |metricClass|
				!@logic[logic][metricClass].nil?
			end
			
		end
		
		def initialize()
			@nilLogger = Logger.new(File::NULL)
			@logger = Yojimbomb::MetricsKeeper::DefaultLogger
			
			@metricClassTrack = Hash.new {|h,mclass| h[mclass] = false}
			@metricTypeTrack = Hash.new do |h1,mclass|
				h1[mclass] = Hash.new {|h2,mtype| h2[mtype] = false}
			end
			
			@logic = Hash.new {|h,k| h[k] = Hash.new }
			
			[:event, :period].each do |metricClass|
			[:ensureMetricClass, :ensureMetricType,:store, :getMetricTypes, :find, :remove].each do |logic|	
				clname = logic.to_s[0].capitalize + logic.to_s[1..-1]
				myDefineMeth = "define#{clname}".to_sym
				classLogicMeth = "#{logic}Logic".to_sym
				send(myDefineMeth, metricClass, &self.class.send(classLogicMeth, metricClass))
			end end
		end
		
		def tryBlock(*failMessages, &block)
			begin
				block.call()
			rescue => e
				emssg = "#{e.message}\n\t#{e.backtrace.join("\n\t")}"
				self.logger.error(emssg)
				failMessages.each {|failMessage| self.logger.error(failMessage)} 
			end
		end	
		
		def handleError(e, message = nil)
			emssg = "#{e.message}\n\t#{e.backtrace.join("\n\t")}"
			self.logger.error(emssg)
			self.logger(message) unless message.nil? 
		end
		
		def ensureMetricClass(metricClass)
			return true if @metricClassTrack[metricClass]
			
			ensured = false
			self.tryBlock("unable to ensure persistence for class=#{metricClass}") do
				logic = @logic[:ensureMetricClass][metricClass]
				return false if logic.nil?
				ensured = self.instance_exec(metricClass, &logic)
			end
			
			ensured = ensured ? true : false
			@metricClassTrack[metricClass] = ensured
		end
		
		def ensureMetricType(metricType, metricClass)
			emc = self.ensureMetricClass(metricClass)
			return false unless emc
			return true if @metricTypeTrack[metricClass][metricType]
			
			ensured = false
			self.tryBlock("unable to ensure persistence for #{metricType}/#{metricClass}") do
				logic = @logic[:ensureMetricType][metricClass]
				ensured = case logic.arity
					when 1 then self.instance_exec(metricType, &logic)
					when 2 then self.instance_exec(metricType, metricClass, &logic)
				end
			end
			
			ensured = ensured ? true : false
			@metricTypeTrack[metricClass][metricType] = ensured
		end
		
		#store given metrics into persistence
		#	this method accepts all kinds of metrics, and will sort to the appropriate delegate method
		#		(eg. storeEvents, storePeriods)
		#
		#	Note that a silent failure approach is taken, metricsKeeper will attempt to store all other metrics,
		#		and failure of a given metric to be persisted will be recorded in the logs.
		#		No ERROR will be thrown, and store() should complete successfully
		#		this behavior applies only to metrics whose persistence can not be ensured (ensurePersistence),
		#		and metrics whose metricClass does not evaluate to a valid, handled value
		#		other errors are assumed to be handeld in the same manner, however this is dependent on the implementation
		#		of storeEvents, storePeriods, etc.
		#		
		def store(*metrics)
			groups = Hash.new do |h1,mclass|
				h1[mclass] = Hash.new do |h2,mtype|
					h2[mtype] = Array.new
				end
			end
				
			metrics.each do |metric| 
				mtype = metric.metricType
				mclass = metric.metricClass
				ensured = self.ensureMetricType(mtype,mclass)
				
				unless ensured
					self.logger.error("unable to store metric(#{mtype}/#{mclass}) #{metric}")
					next
				end
				
				groups[mclass][mtype] << metric
			end
			
			groups.each do |mclass,mtypes| 
				logic = @logic[:store][mclass]
				mtypes.each do |mtype, typMetrics|
					failMessages = typMetrics.map {|metric| "unable to store metric(#{mclass}/#{mtype}): #{metric}"}
					 
					self.tryBlock(*failMessages) do case logic.arity
						when -2 then self.instance_exec(mtype, *typMetrics, &logic)
						when -3 then self.instance_exec(mtype, mclass, *typMetrics, &logic)
					end end 
				end
			end

			self
		end
		
		def metricTypes(metricClass)
			ensured = self.ensureMetricClass(metricClass)
			unless ensured
				self.logger.error("unable to find metric-types for metric-class=#{mclass}")
				return []
			end
			
			self.tryBlock("unable to find metric-types for metric-class=#{mclass}") do
				logic = @logic[:getTypes][metricClass]
				return case logic.arity
					when 0 then self.instance_exec(&logic)
					when 1 then self.instance_exec(metricClass, &logic) 
				end	
			end
			
			[]
		end
		
		def find(metricType, metricClass, criteria = nil)
			ensured = self.ensureMetricType(metricType, metricClass) 
			unless ensured
				self.logger.error("unable to find metrics of #{metricType}/#{metricClass}")
				return []
			end
			
			self.tryBlock("unable to find metrics of #{metricType}/#{metricClass}") do
				logic = @logic[:find][metricClass]
				prefiltered = case logic.arity
					when 2 then self.instance_exec(metricType, criteria, &logic)
					when 3 then self.instance_exec(metricType, metricClass, criteria, &logic)
				end

				return criteria.nil? ? prefiltered : criteria.filter(*prefiltered)
			end
			
			[]
		end
		
		def replace(combMetric, *replIds)
			self.remove(combMetric.metricType, combMetric.metricClass, *replIds)
			self.store(combMetric)
			self
		end
		
		def remove(metricType, metricClass, *metricIds)
			failMessage = "unable to remove metrics of #{metricType}/#{metricClass}: #{metricIds.join(';')}"
			
			ensured = self.ensureMetricType(metricType,metricClass)
			unless ensured
				self.logger.error(failMessage)
				return self
			end
			
			self.tryBlock(failMessage) do
				logic = @logic[:remove][metricClass]
				case logic.arity
					when -2 then self.instance_exec(metricType, *metricIds, &logic)
					when -3 then self.instance_exec(metricType, metricClass, *metricIds, &logic)
				end
			end
			
			self
		end
		
	end
	
end

