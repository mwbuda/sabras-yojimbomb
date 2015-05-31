DESC = <<-EOD
A custom data model interface for Ruby, 
designed to allow flexible implemenations against arbitrary data sources in Rails.
Swapping implementations is done via a package-based system.
EOD

Gem::Specification.new do |gem|
	gem.authors = 'Sabras Soft LLC'
	gem.name = 'yojimbomb'
	gem.version = '0.0.0'
	gem.date = Date.today.to_s
	gem.summary = 'Sabras Yojimbomb: Flexible Metrics Data'
	gem.description = DESC
	gem.files = Dir[
		'lib/**/*',
	]
	gem.license = 'MIT'
end