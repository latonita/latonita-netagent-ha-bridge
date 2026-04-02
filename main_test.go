package main

import (
	"encoding/json"
	"fmt"
	"testing"
)

const statusHTML = `<html><head></head><body><h1>Current Status</h1><div align="left"><b>UPS Status:</b>&nbsp;&nbsp;UPS Normal</div><div align="left"><b>AC Status:&nbsp;&nbsp;</b>Normal<br><b>Input Line Voltage:</b>&nbsp;&nbsp;225.1 V<br><b>Input Max. Line Voltage:</b>&nbsp;&nbsp;231.6 V<br><b>Input Min. Line Voltage:&nbsp;&nbsp;</b>218.7 V<br><b>Input Frequency:&nbsp;&nbsp;</b>49.9 Hz</div><div align="left"><b>Output Voltage:&nbsp;&nbsp;</b>220.0 V<br><b>Output Status:&nbsp;&nbsp;</b>Online<br><b>UPS load:&nbsp;&nbsp;</b>23 %</div><div align="left"><b>Temperature:</b>&nbsp;&nbsp;28.0 °C ( 82.4 °F )<br><b>Battery Status:</b>&nbsp;&nbsp;Battery Normal<br><b>Battery Capacity:</b>&nbsp;&nbsp;100 %<br><b>Battery Voltage:</b>&nbsp;&nbsp;54.96 V<br><b>Time on Battery:&nbsp;&nbsp;</b>00:00:00<br><b>Estimated Battery Remaining Time:&nbsp;&nbsp;</b>00:00:00<br><b>UPS Last Self Test:&nbsp;&nbsp;</b>--<br><b>UPS Next Self Test:&nbsp;&nbsp;</b>--</div></body></html>`

const systemHTML = `<html><head></head><body><form name=form1 method=post><div align="left"><b>Hardware Version:</b>&nbsp;&nbsp;HDP520<br><b>Firmware Version:&nbsp;&nbsp;</b>2.46.DP520.WEST<br><b>Serial Number:&nbsp;&nbsp;</b>3926842903<br><b>System Name:&nbsp;&nbsp;</b>Boiler-UPS<br><b>System Contact:&nbsp;&nbsp;</b>Administrator<br><b>Location:&nbsp;&nbsp;</b>Basement<br><b>System Time:&nbsp;&nbsp;</b><input type=hidden name=$year_date_time value="2026/04/02 00:18:47"><span id=sys_time>2026/04/02 00:18:47</span><br><b>Uptime:&nbsp;&nbsp;</b><input type=hidden name=$up_time_hidden value="198620"><span id=up_time>2 day(s) 07:10:20</span><br><b>UPS Last Self Test:</b>&nbsp;&nbsp;--<br><b>UPS Next Self Test:</b>&nbsp;&nbsp;--<br><b>MAC Address:</b><font style="text-transform=uppercase">&nbsp;&nbsp;00:03:EA:0E:DE:17</font><br><b>IP Address:</b>&nbsp;&nbsp;192.168.8.176<br><b>Email Server:</b>&nbsp;&nbsp;<br><b>Primary DNS Server:</b>&nbsp;&nbsp;192.168.8.1<br><b>Secondary DNS Server:</b>&nbsp;&nbsp;<br><b>PPPoE IP:</b><b>&nbsp;&nbsp;</b><br></div></form></body></html>`

const infoHTML = `<html><head></head><body><div align="left"><b>UPS Manufacturer:&nbsp;&nbsp;</b>Sipower        <br><b>UPS Firmware Version:</b>&nbsp;&nbsp;C1.1.12   <br><b>UPS Model:</b>&nbsp;&nbsp;SIPB1.5BA <br></div><div align="left"><b>Date of last battery replacement:</b>&nbsp;&nbsp;2024/08/26<br><b>Number of Batteries:&nbsp;&nbsp;</b>4<br><b>Battery Charge Voltage:</b>&nbsp;&nbsp;2.267V</div><div align="left"><b>Battery Voltage Rating:</b>&nbsp;&nbsp;48.0V<br></div></body></html>`

func dump(label string, m DataMap) {
	b, _ := json.MarshalIndent(m, "", "  ")
	fmt.Printf("--- %s ---\n%s\n", label, b)
}

func TestParseAll(t *testing.T) {
	cfg := Config{UPSIP: "192.168.8.176"}

	status := parseStatusPage(statusHTML)
	dump("STATUS", status)

	system := parseSystemStatusPage(systemHTML, cfg)
	dump("SYSTEM", system)

	info := parseUpsInfoPage(infoHTML)
	dump("INFO", info)

	// Basic assertions
	if status["inputVoltage"] != 225.1 {
		t.Errorf("inputVoltage: got %v", status["inputVoltage"])
	}
	if status["upsLoadPercentage"] != float64(23) {
		t.Errorf("upsLoadPercentage: got %v", status["upsLoadPercentage"])
	}
	if system["uptimeSeconds"] != 198620 {
		t.Errorf("uptimeSeconds: got %v", system["uptimeSeconds"])
	}
	if system["systemName"] != "Boiler-UPS" {
		t.Errorf("systemName: got %v", system["systemName"])
	}
	if info["upsModel"] != "SIPB1.5BA" {
		t.Errorf("upsModel: got %v", info["upsModel"])
	}
}
