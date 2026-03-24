INSTALL_DIR ?= /usr/local/bin

.PHONY: build install uninstall clean

build:
	go build -o worb .

install: build
	sudo ./install.sh

uninstall:
	sudo systemctl disable --now worb || true
	sudo rm -f /etc/systemd/system/worb.service
	sudo rm -f $(INSTALL_DIR)/worb
	sudo systemctl daemon-reload

clean:
	rm -f worb
