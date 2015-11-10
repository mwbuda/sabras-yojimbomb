
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
			class << self
				define_method("define#{logic.to_s.capitalize}".to_sym) do |metricClass, &block| 
					self.checkConfiguredLogic(*expArity, &block)
					field = "@_mklogic_#{logic}".to_sym
					instance_variable_set(field, {}) if instance_variable_get(field).nil?
					field[metricClass] = block
					self
				end
				
				define_method("#{logic}Logic") do |metricClass|
					field = "@_mklogic_#{logic}".to_sym
					instance_variable_set(field, {}) if instance_variable_get(field).nil?
					field[metricClass]
				end
			end
			
			define_method("define#{logic.to_s.capitalize}".to_sym) do |metricClass, &block|
				self.checkConfiguredLogic(*expArity, &block)
				field = "@_mklogic_#{logic}".to_sym
				instance_variable_get(field)[metricClass] = block
				self
			end
			
			define_method("hasDefined#{logic.to_s.capitalize}?".to_sym) do |metricClass|
				field = "@_mklogic_#{logic}".to_sym
				!instance_variable_get(field)[metricClass].nil?
			end
			
		end
		
		def initialze()
			@nilLogger = Logger.new(File::NULL)
			@logger = Yojimbomb::MetricsKeeper::DefaultLogger
			
			@ensuredMetricClasses = Hash.new {|h,mclass| h[mclass] = false}
			@ensuredMetricTypes = Hash.new do |h1,mclass|
				h1[mclass] = Hash.new {|h2,mtype| h2[mtype] = false}
			end
				
			[:event, :period].each do |metricClass|
			[:ensureMetricClass, :ensureMetrictype,:store, :getMetricTypes, :find, :remove].each do |logic|	
				instance_variable_set("@_mklogic_#{logic}".to_sym, {})
				myDefineMeth = "define#{logic.to_s.capitalize}".to_sym
				classLogicMeth = "#{logic}Logic".to_sym
				send(myDefineMeth, metricClass, &self.class.send(classLogicMeth, metricClass))
			end end
		end
		
		def ensureMetricClass(metricClass)
			return true if @ensuredMetricClasses[metricClass]
			
			ensured = false
			begin
				ensured = self.instance_exec(metricClass, &@ensureMetricClass)
			rescue => e
				self.logger.error(e.message)
				self.logger.error("unable to ensure persistence for class=#{metricClass}")
			end
			
			ensured = ensured ? true : false
			@ensuredMetricClasses[metricClass] = ensured
		end
		
		def ensureMetricType(metricType, metricClass)
			emc = self.ensureMetricClass(metricClass)
			return false unless emc
			return true if @ensuredMetricTypes[metricClass][metricType]
			
			ensured = false
			begin
				logic = @ensureMetricType[metricClass]
				ensured = case logic.arity
					when 1 then self.instance_exec(metricType, &logic)
					when 2 then self.instance_exec(metricType, metricClass, &logic)
				end
			rescue => e
				self.logger.error(e.message)
				self.logger.error("unable to ensure persistence for #{metricType}/#{metricClass}")
			end
			
			ensured = ensured ? true : false
			@ensuredMetricTypes[metricClass][metricType] = ensured
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
				logic = @store[mclass]
				mytypes.each do |mtype, typMetrics| begin
					case logic.arity
						when -2 then self.instance_exec(mtype, *typMetrics, &logic)
						when -3 then self.instance_exec(mtype, mclass, *typMetrics, &logic)
					end
				rescue => e
					typMetrics.each {|metric| self.logger.error("unable to store metric(#{mclass}/#{mtype}): #{metric}") }
				end end
			end

			self
		end
		
		def metricTypes(metricClass)
			ensured = self.ensureMetricClass(metricClass)
			unless ensured
				self.logger.error("unable to find metric-types for metric-class=#{mclass}")
				return []
			end
			 
			begin
				logic = @getTypes[metricClass]
				return case logic.arity
					when 0 then self.instance_exec(&logic)
					when 1 then self.instance_exec(metricClass, &logic) 
				end
			rescue => e
				self.logger.error(e.message)
				self.logger.error("unable to find metric-types for metric-class=#{mclass}")
			end
			
			[]
		end
		
		def find(metricType, metricClass, criteria = nil)
			ensured = self.ensureMetricType(metricType, metricClass) 
			unless ensured
				self.logger.error("unable to find metrics of #{metricType}/#{metricClass}")
				return []
			end
			
			begin
				logic = @find[metricClass]
				prefiltered = case logic.arity
					when 2 then self.instance_exec(metricType, criteria, &logic)
					when 3 then self.instance_exec(metricType, metricClass, criteria, &logic)
				end

				return criteria.nil? ? prefiltered : criteria.filter(prefiltered)
			rescue => e
				self.logger.error(e.message)
				self.logger.error("unable to find metrics of #{metricType}/#{metricClass}")
			end
			
			[]
		end
		
		def replace(combMetric, *replIds)
			self.remove(combMetric.metricType, combMetric.metricClass, *replIds)
			self.store(combMetric)
			self
		end
		
		def remove(metricType, metricClass, *metricIds)
			ensured = self.ensureMetricType(metricType,metricClass)
			unless ensured
				self.logger.error("unable to remove metrics of #{mtype}/#{mclass}")
				return self
			end
			
			begin
				logic = @remove[metricClass]
				case logic.arity
					when -2 then self.instance_exec(metricType, *metricIds, &logic)
					when -3 then self.instance_exec(metricType, metricClass, *metricIds, &logic)
				end
			rescue => e
				self.logger.error(e.message)
				self.logger.error("unable to remove metrics of #{mtype}/#{mclass}: #{metricIds.join(';')}")
			end
			
			self
		end
		
	end
	
end

