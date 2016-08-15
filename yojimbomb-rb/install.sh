clear
sudo gem uninstall yojimbomb-rdbms
sudo gem uninstall yojimbomb
gem build yojimbomb.gemspec
gem build yojimbomb-rdbms.gemspec
sudo gem install yojimbomb-1.1.0.gem
sudo gem install yojimbomb-rdbms-1.1.0.gem
