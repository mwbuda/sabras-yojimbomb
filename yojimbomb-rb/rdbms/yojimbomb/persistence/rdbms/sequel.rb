
require 'yojimbomb'

require 'sequel'

module Yojimbomb
module RDBMS
	
	#
	# Sequel based RDBMS integration
	# 
	# Implemenation notes:
	# 	* uses a dynamic table creation system
	# 	* per implemenation, minor tags are effectivly indexed
	#
	class SequelMetricsKeeper < Yojimbomb::MetricsKeeper
		
		#TODO: eventually use this to swap out column types & other per-dialect adaptations
		SupportedDialects = [
			:mysql, :mysql2,
		]
		
		attr_accessor :tablePrefix
		
		def self.connect(*connect, &cblock)
			self.new( Sequel.connect(*connect, &cblock) )
		end
		
		def initialize(db, dialect = nil)
			super()
			Sequel.default_timezone = :utc
			@db = db
			@dialect = dialect.nil? ? @db.adapter_scheme : dialect
			raise "invalidDbDialect: #{@dialect}" unless SupportedDialects.include?(@dialect)
			
			@dataTypes = {
				:uint => 'integer(10) unsigned',
				:symbol => 'char(10)',
				:searchLabel => 'char(32)',
				:timestamp => 'timestamp',
				:todValue => 'integer(4) unsigned'
			}
		end
		
		def serializeId(id)
			hex = id.to_s(16)
			hex.insert(0, '0' * (32 - hex.size))
			hex.scan(/.{8,8}/).map {|byte| byte.hex}
		end
		def parseId(id1,id2,id3,id4)
			[id1,id2,id3,id4].map do |idp| 
				hex = idp.to_s(16)
				hex.insert(0, '0' * (8 - hex.size))
				hex
			end.join.to_i(16) 
		end
		
		def cleanDbSym(id) 
			id.to_s.downcase.strip[0..9]
		end
		
		def tableName(*nameParts)
			prefix = self.cleanDbSym( @tablePrefix.nil? ? '' : "#{@tablePrefix}_")
			cnparts = nameParts.map {|p| cleanDbSym(p) }
			"#{prefix}#{cnparts.join('_')}".to_sym
		end
		
		[:period,:event].each do |mc| defineEnsureMetricClass(mc) do |metricClass|
			dataTypes = @dataTypes
			@db.create_table? tableName('meta', metricClass) do
				primary_key(:pk, dataTypes[:uint])
				column(:metricType, dataTypes[:symbol], :unique => true)
			end
			self
		end end
		
		def createSearchTables(mtype,mclass)
			dataTypes = @dataTypes
			
			mainSearchTable = tableName(mclass,mtype,'sch')
			ptagSearchTable = tableName(mclass,mtype,'pti')
			mtagSearchTable = tableName(mclass,mtype,'mti')
			rmTable = tableName(mclass,mtype,'rmi')
			
			@db.create_table? mainSearchTable do 
				primary_key(:pk, dataTypes[:uint])
				column(:label, dataTypes[:searchLabel])
				column(:created, dataTypes[:timestamp])
			end
			
			[ptagSearchTable, mtagSearchTable].each do |tagSearchTable| @db.create_table? tagSearchTable do
				primary_key(:pk, :type => dataTypes[:uint])
				foreign_key(:sid, mainSearchTable, :type => dataTypes[:uint], :null => false, :on_delete => :cascade)
				column(:tagv, dataTypes[:symbol], :null => false)
				unique([:sid, :tagv])
			end end
			
			@db.create_table? rmTable do
				primary_key(:pk, dataTypes[:uint])
				foreign_key(:sid, mainSearchTable, :type => dataTypes[:uint], :null => false, :on_delete => :cascade)
					
				column(:mid1, dataTypes[:uint], :null => false)
				column(:mid2, dataTypes[:uint], :null => false)
				column(:mid3, dataTypes[:uint], :null => false)
				column(:mid4, dataTypes[:uint], :null => false)
				
				unique([:sid, :mid1, :mid2, :mid3, :mid4])
			end
			
		end
		
		def createTagTables(mtype, mclass)
			dataTypes = @dataTypes
			
			#minor tag main, link, & search tables
			mtags  = tableName(mclass,mtype,'mt')
			mtagsx = tableName(mclass,mtype,'mtx')
			
			#primary tag main, link, & search tables
			ptags  = tableName(mclass,mtype,'pt')
			ptagsx = tableName(mclass,mtype,'ptx')
			
			[ [ptags,ptagsx], [mtags,mtagsx] ].each do |tags,tagx|
				@db.create_table? tags do 
					primary_key(:pk, dataTypes[:uint])
					column(:tagv, dataTypes[:symbol], :unique => true)
				end
				@db.create_table? tagx do
					foreign_key(:tid, tags, :type => dataTypes[:uint], :null => false, :on_delete => :cascade)
					
					column(:mid1, dataTypes[:uint], :null => false)
					column(:mid2, dataTypes[:uint], :null => false)
					column(:mid3, dataTypes[:uint], :null => false)
					column(:mid4, dataTypes[:uint], :null => false)
					
					primary_key([:tid,:mid1,:mid2,:mid3,:mid4])
				end
			end
		end
		
		defineEnsureMetricType(:event) do |metricType, mclass|
			dataTypes = @dataTypes
			
			meta = tableName('meta', mclass)
			table  = tableName(mclass,metricType)
			rmTracker = tableName(mclass,metricType, 'rm')
			
			if @db[meta][:metricType => cleanDbSym(metricType)].nil?
				@db[meta] << {:metricType => cleanDbSym(metricType)}
					
				@db.create_table?(table) do
					column(:id1, dataTypes[:uint], :null => false)
					column(:id2, dataTypes[:uint], :null => false)
					column(:id3, dataTypes[:uint], :null => false)
					column(:id4, dataTypes[:uint], :null => false)
					primary_key([:id1,:id2,:id3,:id4])
					
					column(:count, dataTypes[:uint], :null => false)
					column(:occur, dataTypes[:timestamp])
					column(:qty, dataTypes[:uint], :null => false)
				end
				
				createTagTables(metricType, mclass)
				createSearchTables(metricType, mclass)
			end
			
			self
		end
		
		defineEnsureMetricType(:period) do |metricType, metricClass|
			dataTypes = @dataTypes
			
			meta = tableName('meta', metricClass)
			table  = tableName(metricClass, metricType)
			rmTracker = tableName(metricClass, metricType, 'rm')
			
			if @db[meta][:metricType => cleanDbSym(metricType)].nil?
				@db[meta] << {:metricType => cleanDbSym(metricType)}
					
				@db.create_table?(table) do
					column(:id1, dataTypes[:uint], :null => false)
					column(:id2, dataTypes[:uint], :null => false)
					column(:id3, dataTypes[:uint], :null => false)
					column(:id4, dataTypes[:uint], :null => false)
					primary_key([:id1,:id2,:id3,:id4])
					
					column(:count, dataTypes[:uint], :null => false)
					[:pstart,:pstop].each {|col| column(col, dataTypes[:timestamp]) }
					[:todstart,:todstop].each {|col| column(col, dataTypes[:todValue])}
					column(:dur, dataTypes[:uint])
				end
				
				createTagTables(metricType, metricClass)
				createSearchTables(metricType, metricClass)
			end
			
			self
		end
		
		defineStore(:event) do |mtype, *events|
			mclass = :event
			table = tableName(mclass, mtype)
			events.each do |event|
			self.tryBlock("unable to store metric(#{mtype}/#{mclass}) #{event}") do
				ids = serializeId(event.id)
				@db[table] << {
					:id1 => ids[0], :id2 => ids[1], :id3 => ids[2], :id4 => ids[3],
					:count => event.count, 
					:occur => event.occurence,
					:qty => event.quantity
				}
				self.persistTags(event)
			end end
			
			self
		end
		
		defineStore(:period) do |mtype, *periods|
			mclass = :period
			table = tableName(mclass, mtype)
			
			periods.each do |period|
			self.tryBlock("unable to store metric(#{mtype}/#{mclass}) #{period}") do
				ids = serializeId(period.id)
				@db[table] << {
					:id1 => ids[0], :id2 => ids[1], :id3 => ids[2], :id4 => ids[3],
					:count => period.count, 
					:pstart => period.start, :pstop => period.stop,
					:todstart => (period.startTimeOfDay * 100).to_i, :todstop => (period.stopTimeOfDay * 100).to_i,
					:dur => period.duration,
				}
				self.persistTags(period)
			end end
			
			self
		end
		
		def persistTags(metric)
			mtype, mclass = metric.metricType, metric.metricClass
			ids = serializeId(metric.id)
			
			metricTb = tableName(mclass, mtype)
			mtb  = tableName(mclass, mtype, 'mt')
			mxtb = tableName(mclass, mtype, 'mtx')
			ptb  = tableName(mclass, mtype, 'pt')
			pxtb = tableName(mclass, mtype, 'ptx')
			
			[
				[mtb, mxtb, metric.minorTags],
				[ptb, pxtb, metric.primaryTags]
			].each do |tb, xtb, tags| tags.uniq.each do |tag|
				tagv = cleanDbSym(tag)
				
				tagid = nil
				tagRow = @db[tb][:tagv => tagv]
				tagid = tagRow[:pk] unless tagRow.nil?
				if tagid.nil?
					@db[tb] << {:tagv => tagv}
					tagid = @db[tb][:tagv => tagv][:pk]
				end
				
				noLink = @db[xtb][:tid => tagid, :mid1 => ids[0], :mid2 => ids[1], :mid3 => ids[2], :mid4 => ids[3] ].nil?
				@db[xtb] << {:tid => tagid, :mid1 => ids[0], :mid2 => ids[1], :mid3 => ids[2], :mid4 => ids[3]}	
			end end
			
			self
		end
		
		def openSearch(metricType, metricClass)
			searchMainTable = tableName(metricClass, metricType, 'sch')
			slabel = Yojimbomb.idValue.to_s(16)
			@db[searchMainTable] << {:label => slabel, :created => Time.now}
			@db[searchMainTable][:label => slabel][:pk]
		end
		
		def closeSearch(metricType, metricClass, searchId)
			searchMainTable = tableName(metricClass, metricType, 'sch')
			@db[searchMainTable].where(:pk => searchId).delete
		end
		
		[:event,:period].each do |mclass| defineGetMetricTypes(mclass) do
			meta = tableName('meta',mclass)
			@db[meta].map(:metricType).map {|mt| mt.to_sym}
		end end
		
		def filterFindByTags(metricType, metricClass, criteria, searchId, dataset)
			dset = dataset
			{
				'p' => criteria.primaryTags,
				'm' => criteria.minorTags
			}.each do |tx, tags|
				next if tags.empty?
				tagMnTb = tableName(metricClass, metricType, "#{tx}t")
				tagLnTb = tableName(metricClass, metricType, "#{tx}tx")
				tagTsTb = tableName(metricClass, metricType, "#{tx}ti")
				
				temps = tags.map do |tag|
					{:sid => searchId, :tagv => cleanDbSym(tag)}
				end
				@db[tagTsTb].multi_insert(temps)
				
				tagvFilterClause = "tid in (select pk from #{tagMnTb} where tagv in (select tagv from #{tagTsTb} where sid=?))"
				idFilterClause = [1,2,3,4].map {|i| "(mid#{i}=id#{i})"}.join(' and ')
				
				dset = dset.where("( select count(*) from #{tagLnTb} where (#{tagvFilterClause}) and #{idFilterClause} ) > 0", searchId)
			end
			
			dset
		end
		
		def populateTags(metric)
			metricType, metricClass = metric.metricType, metric.metricClass
			ids =  serializeId(metric.id)
			
			pMnTbl = tableName(metricClass, metricType, 'pt')
			pLnTbl = tableName(metricClass, metricType, 'ptx')
			
			mMnTbl = tableName(metricClass, metricType, 'mt')
			mLnTbl = tableName(metricClass, metricType, 'mtx')
			
			filterClause = [:mid1,:mid2,:mid3,:mid4].map {|mid| "(#{mid}=?)"}.join(' and ')
			
			rawpts = @db[pMnTbl].where(
				"pk in (select tid from #{pLnTbl} where #{filterClause})", *ids
			).map(:tagv).map {|tagv| tagv.to_sym}
			metric.withPrimaryTags(*rawpts)
			
			rawmts = @db[mMnTbl].where(
				"pk in (select tid from #{mLnTbl} where #{filterClause})", *ids
			).map(:tagv).map {|tagv| tagv.to_sym}
			metric.withMinorTags(*rawmts)
			
		end
		
		defineFind(:event) do |metricType, criteria|
			table = tableName(:event,metricType)
			searchId = openSearch(metricType, :event)
			dset = @db[table]
			
			#initial, dataset based filtering
			#	(in other words, in the db itself)
			#	note that this does NOT include dow & tod based filtering,
			#	which is done post-query to avoid extra dicking around w/ timezones in db
			unless criteria.nil?
				dset = dset.where(:occur => (criteria.start..criteria.stop))
				dset = filterFindByTags(metricType, :event, criteria, searchId, dset)
			end
			
			#map to actual events
			res = []
			dset.each do |raw|
				occur = raw[:occur]
				sundry = {
					:id => parseId(raw[:id1], raw[:id2], raw[:id3], raw[:id4]),
					:count => raw[:count],
					:quantity => raw[:qty],
				}
				res << Yojimbomb::EventMetric.new(metricType, occur, sundry)
			end
			
			#populate tags
			res.each {|metric| populateTags(metric) }
			
			closeSearch(metricType, :event, searchId)
			res
		end
		
		defineFind(:period) do |metricType, criteria|
			table = tableName(:period,metricType)
			searchId = openSearch(metricType,:period)
			dset = @db[table]
			
			#initial, dataset based filtering
			#	(in other words, in the db itself)
			#	note that this does NOT include dow & tod based filtering,
			#	which is done post-query to avoid extra dicking around w/ timezones in db
			unless criteria.nil?
				dset = dset.where {:pstart >= criteria.start}
				dset = dset.where {:pstop <= criteria.stop}
				dset = filterFindByTags(metricType, :period, criteria, searchId, dset)
			end
			
			#map to actual periods
			res = []
			dset.each do |raw|
				start = raw[:pstart]
				stop = raw[:pstop]
				duration = raw[:dur]
				
				sundry = {
					:id => parseId(raw[:id1], raw[:id2], raw[:id3], raw[:id4]),
					:count => raw[:count],
					:todStart => ( (raw[:todstart] / 100) + ((raw[:todstart] % 100).to_f / 100.0) ).to_f,
					:todStop => ( (raw[:todstop] / 100) + ((raw[:todstop] % 100).to_f / 100.0) ).to_f
				}
				res << Yojimbomb::PeriodMetric.new(metricType, start, stop, duration, sundry)
			end
			
			#populate tags
			res.each {|metric| populateTags(metric) }
			
			closeSearch(metricType, :period, searchId)
			res
		end
		
		[:event, :period].each do |metricClass| defineRemove(metricClass) do |metricType, mclass, *ids|
			mnTable = tableName(mclass,metricType)
			rmTable = tableName(mclass,metricType,'rmi') 
			
			searchId = openSearch(metricType, mclass) 
			
			serializedIds = {}
			
			rmTrackItems = ids.map do |id|
				xids = serializeId(id)
				serializedIds[id] = xids
				{:sid => searchId, :mid1 => xids[0], :mid2 => xids[1], :mid3 => xids[2], :mid4 => xids[3]}
			end
			@db[rmTable].multi_insert(rmTrackItems)
			
			filterClause = [1,2,3,4].map {|i| "mid#{i}=id#{i}"}.join(' and ')			
			@db[mnTable].where("(select count(*) from #{rmTable} where (sid=?) and #{filterClause}) > 0", searchId).delete
			
			closeSearch(metricType, mclass, searchId)
			
			self
		end end
	
	end
	
end end 

