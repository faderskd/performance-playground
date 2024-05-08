# -*- mode: ruby -*-
# vi: set ft=ruby :

vagrant_name = "performance-playground"

Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/jammy64"

  config.vm.hostname = vagrant_name

  config.vm.network :forwarded_port, guest: 11211, host: 11211

  config.vm.provision "shell", path: "vagrant/install-tools.sh"
  config.vm.provision "file", source: "/Users/daniel.faderski/Desktop/Wlasne/performance-playground", destination: "/home/vagrant/performance-playground"

  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", "4096"]
    vb.customize ["modifyvm", :id, "--cpus", "4"]
    vb.customize ["modifyvm", :id, "--hpet", "on"]
    vb.name = vagrant_name
  end

end
