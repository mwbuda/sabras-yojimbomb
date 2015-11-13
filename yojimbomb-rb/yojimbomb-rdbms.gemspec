DESC = <<-EOD
Support for integrating RDBMS persistence of metrics for Yojimbomb
EOD

Gem::Specification.new do |gem|
	gem.authors = 'Sabras Soft LLC'
	gem.name = 'yojimbomb-rdbms'
	gem.version = '1.0.0'
	gem.date = Date.today.to_s
	gem.summary = 'Sabras Yojimbomb RDBMS integration'
	gem.description = DESC
	
	gem.files = Dir[
		'rdbms/**/*',
	]
	gem.require_paths = ['rdbms']
	gem.add_runtime_dependency 'yojimbomb', '~> 1.0'
	gem.add_runtime_dependency 'sequel', '~> 4.28'
	
	gem.license = 'MIT'
end