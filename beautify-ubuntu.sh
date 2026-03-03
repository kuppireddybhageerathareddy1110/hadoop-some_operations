#!/bin/bash

# Update system
sudo apt update && sudo apt upgrade -y

# Install basic customization tools
sudo apt install -y gnome-tweaks gnome-shell-extensions gnome-shell-extension-manager variety unzip wget

# Install Papirus icon theme
sudo apt install -y papirus-icon-theme

# Install WhiteSur GTK theme & icons
mkdir -p ~/Themes ~/Icons
wget -qO WhiteSur.tar.xz https://github.com/vinceliuice/WhiteSur-gtk-theme/archive/refs/heads/master.tar.gz
tar -xvf WhiteSur.tar.xz
cd WhiteSur-gtk-theme-master
./install.sh -d ~/Themes
cd ..

# Install WhiteSur icons
wget -qO WhiteSur-icons.tar.xz https://github.com/vinceliuice/WhiteSur-icon-theme/archive/refs/heads/master.tar.gz
tar -xvf WhiteSur-icons.tar.xz
cd WhiteSur-icon-theme-master
./install.sh -d ~/Icons
cd ..

# Install GNOME Extensions (Blur My Shell & Dash to Dock)
sudo apt install -y gnome-shell-extension-blur-my-shell gnome-shell-extension-dash-to-dock

# Enable extensions
gnome-extensions enable blur-my-shell@aunetx
gnome-extensions enable dash-to-dock@micxgx.gmail.com

# Set Papirus icons
gsettings set org.gnome.desktop.interface icon-theme "Papirus"

# Set WhiteSur theme
gsettings set org.gnome.desktop.interface gtk-theme "WhiteSur-dark"

# Launch GNOME Tweaks for final adjustments
gnome-tweaks &

echo "✅ Ubuntu is now BEAUTIFIED! Enjoy your new look."
echo "Tip: Use 'Variety' to set up rotating wallpapers."
