
require 'yojimbomb'

require 'sequel'
require 'securerandom'

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
		
		attr_accessor :tablePrefix
		
		def self.connect(*connect, &cblock)
			self.new( Sequel.connect(*connect, &cblock) )
		end
		
		def initialize(db)
			super()
			@db = db
		end
		
		def timeDbStr(ts)
			#TODO
		end
		
		def cleanDbId(id) 
			id.to_s.downcase.strip[0..9]
		end
		
		def tableName(*nameParts)
			prefix = self.cleanDbId( @tablePrefix.nil? ? '' : "#{@tablePrefix}_")
			cnparts = nameParts.map {|p| cleanDbId(p) }
			"#{prefix}#{cnparts.join('_')}".to_sym
		end
		
		[:period,:event].each do |mc| defineEnsureMetricClass(mc) do |metricClass|
			@db.create_table? tableName('meta', metricClass) do
				column(:id,'binary(16)', :primary_key => true)
				column(:metricType, 'varchar(10)', :unique => true)
			end
			self
		end end
		
		def createTagTables(mtype, mclass)
			table  = tableName(mclass,mtype)
			
			#minor tag main, link, & search tables
			mtags  = tableName(mclass,mtype,'mt')
			mtagsx = tableName(mclass,mtype,'mtx')
			mtagss = tableName(mclass,mtype,'mts')
			
			#primary tag main, link, & search tables
			ptags  = tableName(mclass,mtype,'pt')
			ptagsx = tableName(mclass,mtype,'ptx')
			ptagss = tableName(mclass,mtype,'pts')
			
			[ [ptags,ptagsx,ptagss], [mtags,mtagsx,mtagss] ].each do |tags,tagx, tagss|
				@db.create_table? tags do 
					column(:id, 'binary(16)', :primary_key => true)
					column(:tagv, 'varchar(10)', :unique => true)
				end
				@db.create_table? tagx do
					column(:id, 'binary(16)', :primary_key=>true)
					{:tag => tags, :metric => table}.each do |col,xtab|
						column(col,'binary(16)')
						foreign_key(col, xtab, :null => false)
					end
					index([:metric, :tag])
				end
				@db.create_table? tagss do
					column(:id, 'binary(16)', :primary_key => true)
					column(:sid, 'binary(16)', :null => false)
					column(:tagv, 'varchar(10)', :null => false)
				end
			end
		end
		
		defineEnsureMetricType(:event) do |metricType, mclass|
			meta = tableName('meta', mclass)
			table  = tableName(mclass,metricType)
			rmTracker = tableName(mclass,metricType, 'rm')
			
			@db[meta] << {:id => SecureRandom.uuid, :metricType => metricType}
				
			@db.create_table?(table) do
				column(:id, 'binary(16)', :primary_key => true)
				column(:count,:integer)
				column(:occur,:timestamp)
				column(:qty,:integer)
			end
			
			@db.create_table?(rmTracker) do
				column(:id, 'binary(16)', :primary_key => true)
				column(:sid, 'binary(16)')
				column(:metric, 'binary(16)')
			end
			
			createTagTables(metricType, mclass)
			self
		end
		
		defineEnsureMetricType(:period) do |metricType, metricClass|
			meta = tableName('meta', metricClass)
			table  = tableName(metricClass, metricType)
			
			@db[meta] << {:id => SecureRandom.uuid, :metricType => cleanDbId(metricType)}
				
			@db.create_table?(table) do
				column(:id, 'binary(16)', :primary_key => true)
				column(:count,:integer)
				[:pstart,:pstop].each {|col| column(col,:timestamp)}
				[:todstart,:todstop].each {|col| column(col,'numeric(4,2)')}
				column(:dur,:integer)
			end
			
			createTagTables(metricType, mclass)
			self
		end
		
		defineStore(:event) do |mtype, *events|
			mclass = :event
			table = tableName(mclass, mtype)
			events.each do |event|
			self.tryBlock("unable to store metric(#{mtype}/#{mclass}) #{event}") do
				@db[table] << {
					:id => event.id, :count => event.count, :occur => event.occurence,
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
				@db[table] << {
					:id => event.id, :count => event.count, 
					:pstart => period.start, :pstop => period.stop,
					:todstart => period.startTimeOfDay, :todstop => period.stopTimeOfDay,
					:dur => period.duration,
				}
				self.persistTags(period)
			end end
			
			self
		end
		
		def persistTags(metric)
			mtype, mclass = metric.metricType, metric.metricClass
			metricTb = tableName(mclass, mtype)
			mtb  = tableName(mclass, mtype, 'mt')
			mxtb = tableName(mclass, mtype, 'mtx')
			ptb  = tableName(mclass, mtype, 'pt')
			pxtb = tableName(mclass, mtype, 'ptx')
			
			[
				[mtb, mxtb, metric.minorTags],
				[ptb, pxtb, metric.primaryTags]
			].each do |tb, xtb, tags| tags.uniq.each do |tag|
				tagid = @db[tb][:tagv => tag].get(:id)
				if tagid.nil?
					tagid = SecureRandom.uuid
					@db[tb] << {:id => tagid, :tagv => cleanDbId(tag)}
				end
				
				noLink = @db[xtb][:tag => tagid, :metric => metric.id].empty?
				@tb[xtb] << {:id => SecureRandom.uuid, :tag => tagid, :metric => metric.id}	
			end end
			
			self
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
				tagTsTb = tableName(metricClass, metricType, "#{tx}ts")
				
				temps = tags.map do |tag|
					{:id => SecureRandom.uuid, :sid => searchId, :tagv => cleanDbId(tag)}
				end
				@db[tagTsTb].multi_insert(temps)
				
				tquery = <<-SQL
					id in ( 
						select metric from #{tagLnTb} where tag in ( 
							select id from #{tagMnTb} where tagv in (
								select tagv from #{tagTsTb} where sid = ?
							)
						)
					)
				SQL
				
				dset = dset.where(tquery, searchId)
			end
			
			dset
		end
		
		def cleanupFind(metricType, metricClass, searchId)
			pTagTsTb = tableName(metricClass, metricType, 'pts')
			mTagTsTb = tableName(metricClass, metricType, 'mts')
			@db[pTagTsTb][:sid => searchId].delete
			@db[mTagTsTb][:sid => searchId].delete
			self
		end
		
		def populateTags(metric)
			metricType, metricClass = metric.metricType, metric.metricClass
			
			pMnTbl = tableName(metricClass, metricType, 'pt')
			pLnTbl = tableName(metricClass, metricType, 'ptx')
			metric.withPimaryTags(@db[pMnTbl].where(
				"id in (select tag from #{pLnTbl} where metric = ?)", metric.id
			).map(:tagv))
			
			mMnTbl = tableName(metricClass, metricType, 'mt')
			mLnTbl = tableName(metricClass, metricType, 'mtx')
			metric.withMinorTags(@db[mMnTbl].where(
				"id in (select tag from #{mLnTbl} where metric = ?)", metric.id
			).map(:tagv))
			
		end
		
		defineFind(:event) do |metricType, criteria|
			table = tableName(:event,metricType)
			searchId = SecureRandom.uuid
			dset = @db[table]
			
			#initial, dataset based filtering
			#	(in other words, in the db itself)
			#	note that this does NOT include dow & tod based filtering,
			#	which is done post-query to avoid extra dicking around w/ timezones in db
			unless criteria.nil?
				dset = dset.where("occur >= #{timeDbStr(criteria.start)}")
				dset = dset.where("occur <= #{timeDbStr(criteria.stop)}")
				dset = filterFindByTags(metricType, :event, criteria, searchId, dset)
			end
			
			#map to actual events
			res = []
			dset.each do |raw|
				occur = raw[:occur]
				sundry = {
					:id => raw[:id],
					:count => raw[:count],
					:quantity => raw[:qty],
				}
				res << Yojimbomb::Event.new(metricType, occur, sundry)
			end
			
			#populate tags
			res.each {|metric| populateTags(metric) }
			
			cleanupFind(metricType, :event, searchId)
			res
		end
		
		defineFind(:period) do |metricType, criteria|
			table = tableName(:period,metricType)
			searchId = SecureRandom.uuid
			dset = @db[table]
			
			#initial, dataset based filtering
			#	(in other words, in the db itself)
			#	note that this does NOT include dow & tod based filtering,
			#	which is done post-query to avoid extra dicking around w/ timezones in db
			unless criteria.nil?
				dset = dset.where("pstart >= #{timeDbStr(criteria.start)}")
				dset = dset.where("pstop <= #{timeDbStr(criteria.stop)}")
				dset = filterFindByTags(metricType, :period, criteria, searchId, dset)
			end
			
			#map to actual events
			res = []
			dset.each do |raw|
				start = raw[:pstart]
				stop = raw[:pstop]
				duration = raw[:dur]
				sundry = {
					:id => raw[:id],
					:count => raw[:count],
					:todStart => raw[:todstart],
					:todStop => raw[:todstop]
				}
				res << Yojimbomb::Period.new(metricType, start, stop, duration, sundry)
			end
			
			#populate tags
			res.each {|metric| populateTags(metric) }
			
			cleanupFind(metricType, :period, searchId)
			res
		end
		
		[:event, :period].each do |metricClass| defineRemove(metricClass) do |metricType, mclass, *ids|
			clearId = SecureRandom.uuid
			mnTable = tableName(mclass,metricType)
			rmTable = tableName(mclass,metricType,'rm') 
			
			rmTrackItems = ids.map do |id|
				{:id => SecureRandom.uuid, :sid => clearId, :metric => id}
			end
			@db[rmTable].multi_insert(rmTrackItems)
			
			@db[mnTable].where("id in (select metric from #{rmTable} where sid = ?)", clearId).delete
			@db[rmTable][:sid => clearId].delete
			
			self
		end end
	
	end
	
end end 

