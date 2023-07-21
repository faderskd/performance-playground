# -*- mode: ruby -*-
# vi: set ft=ruby :

vagrant_name = "queuestd-performance-playground"

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/jammy64"

  config.vm.hostname = vagrant_name

  config.vm.network :forwarded_port, guest: 11211, host: 11211
  config.vm.network :forwarded_port, guest: 5000, host: 6000

  config.vm.provision "shell", path: "vagrant/install-python.sh"

  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", "4096"]
    vb.customize ["modifyvm", :id, "--cpus", "4"]
    vb.customize ["modifyvm", :id, "--hpet", "on"]
    vb.name = vagrant_name
  end

end
