package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/net/html"
)

type Config struct {
	MQTTServer             string
	UPSIP                  string
	UPSHTTPPort            int
	UPSHTTPTimeoutMS       int
	UPSTopic               string
	DiscoveryTopicPrefix   string
	StatusPath             string
	SystemPath             string
	InfoPath               string
	HADeviceID             string
	HADeviceName           string
	ConfigurationURL       string
	PollIntervalMS         int
	Debug                  bool
}

func loadConfig() Config {
	c := Config{
		MQTTServer:       envOr("MQTT_SERVER", "mqtt://192.168.1.1"),
		UPSIP:            envOr("UPS_IP", "192.168.1.2"),
		UPSHTTPPort:      envOrInt("UPS_HTTP_PORT", 80),
		UPSHTTPTimeoutMS: envOrInt("UPS_HTTP_TIMEOUT_MS", 5000),
		UPSTopic:         envOr("UPS_TOPIC", "ups-netagent"),
		StatusPath:       envOr("UPS_STATUS_PATH", "/pda/status-1.htm"),
		SystemPath:       envOr("UPS_SYSTEM_PATH", "/pda/sys_status.htm"),
		InfoPath:         envOr("UPS_INFO_PATH", "/pda/UPS.htm"),
		HADeviceID:       envOr("HA_DEVICE_ID", "ups_netagent"),
		HADeviceName:     os.Getenv("HA_DEVICE_NAME"),
		PollIntervalMS:   envOrInt("POLL_INTERVAL_SECONDS", 20) * 1000,
		Debug:            strings.EqualFold(os.Getenv("DEBUG"), "true"),
	}

	if v := os.Getenv("DISCOVERY_TOPIC_PREFIX"); v != "" {
		c.DiscoveryTopicPrefix = v
	} else {
		c.DiscoveryTopicPrefix = fmt.Sprintf("homeassistant/sensor/%s", c.UPSTopic)
	}

	if v := os.Getenv("UPS_CONFIG_URL"); v != "" {
		c.ConfigurationURL = v
	} else if c.UPSHTTPPort == 80 {
		c.ConfigurationURL = fmt.Sprintf("http://%s", c.UPSIP)
	} else {
		c.ConfigurationURL = fmt.Sprintf("http://%s:%d", c.UPSIP, c.UPSHTTPPort)
	}

	return c
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

// --- HTML fetching ---

func fetchUpsPage(cfg Config, path string) (string, error) {
	addr := fmt.Sprintf("%s:%d", cfg.UPSIP, cfg.UPSHTTPPort)
	conn, err := net.DialTimeout("tcp", addr, time.Duration(cfg.UPSHTTPTimeoutMS)*time.Millisecond)
	if err != nil {
		return "", fmt.Errorf("connect %s: %w", addr, err)
	}
	defer conn.Close()

	deadline := time.Now().Add(time.Duration(cfg.UPSHTTPTimeoutMS) * time.Millisecond)
	conn.SetDeadline(deadline)

	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", path, cfg.UPSIP)
	if _, err := conn.Write([]byte(req)); err != nil {
		return "", fmt.Errorf("write request: %w", err)
	}

	var buf []byte
	tmp := make([]byte, 4096)
	for {
		n, err := conn.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			if err != io.EOF {
				return "", fmt.Errorf("read response: %w", err)
			}
			break
		}
	}

	raw := string(buf)
	idx := strings.Index(raw, "\r\n\r\n")
	if idx == -1 {
		return "", fmt.Errorf("invalid HTTP response from UPS")
	}
	return raw[idx+4:], nil
}

// --- HTML parsing ---

func sanitizeWhitespace(s string) string {
	s = strings.ReplaceAll(s, "\u00a0", " ")
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}

func getTextContent(n *html.Node) string {
	if n.Type == html.TextNode {
		return n.Data
	}
	var sb strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		sb.WriteString(getTextContent(c))
	}
	return sb.String()
}

func getAttr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func collectValueAfterLabel(boldEl *html.Node) string {
	var parts []string
	for node := boldEl.NextSibling; node != nil; node = node.NextSibling {
		if node.Type == html.ElementNode && node.Data == "br" {
			break
		}
		if node.Type == html.TextNode {
			parts = append(parts, node.Data)
		}
		if node.Type == html.ElementNode {
			if node.Data == "input" {
				continue
			}
			parts = append(parts, getTextContent(node))
		}
	}
	val := sanitizeWhitespace(strings.Join(parts, " "))
	if val == "" {
		return ""
	}
	return val
}

func buildLabelValueMap(doc *html.Node) map[string]string {
	labels := map[string]string{}
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "b" {
			labelText := strings.TrimSuffix(sanitizeWhitespace(getTextContent(n)), ":")
			if labelText != "" {
				val := collectValueAfterLabel(n)
				if val != "" {
					labels[labelText] = val
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return labels
}

func findElementByID(n *html.Node, id string) *html.Node {
	if n.Type == html.ElementNode {
		if getAttr(n, "id") == id {
			return n
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if found := findElementByID(c, id); found != nil {
			return found
		}
	}
	return nil
}

func findInputByName(n *html.Node, name string) *html.Node {
	if n.Type == html.ElementNode && n.Data == "input" {
		if getAttr(n, "name") == name {
			return n
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if found := findInputByName(c, name); found != nil {
			return found
		}
	}
	return nil
}

var numberRe = regexp.MustCompile(`-?\d+(?:\.\d+)?`)

func extractFirstNumber(val string, precision int) (float64, bool) {
	if val == "" {
		return 0, false
	}
	m := numberRe.FindString(val)
	if m == "" {
		return 0, false
	}
	n, err := strconv.ParseFloat(m, 64)
	if err != nil {
		return 0, false
	}
	if precision >= 0 {
		factor := math.Pow(10, float64(precision))
		n = math.Round(n*factor) / factor
	}
	return n, true
}

func parseDurationToSeconds(val string) (int, bool) {
	if val == "" || strings.Contains(val, "--") {
		return 0, false
	}
	parts := strings.Split(val, ":")
	if len(parts) != 3 {
		return 0, false
	}
	total := 0
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return 0, false
		}
		total = total*60 + n
	}
	return total, true
}

func toIsoTimestamp(val string) string {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" || trimmed == "--" {
		return ""
	}
	return strings.ReplaceAll(trimmed, "/", "-")
}

// --- Data types ---

// We use map[string]interface{} for flexibility, matching the JS approach.
type DataMap = map[string]interface{}

func setIfNotEmpty(m DataMap, key, val string) {
	if val != "" {
		m[key] = val
	}
}

func setNumber(m DataMap, key, raw string, precision int) {
	if v, ok := extractFirstNumber(raw, precision); ok {
		m[key] = v
	}
}

func setDuration(m DataMap, key, raw string) {
	if v, ok := parseDurationToSeconds(raw); ok {
		m[key] = v
	}
}

func parseHTML(body string) (*html.Node, error) {
	return html.Parse(strings.NewReader(body))
}

func parseStatusPage(body string) DataMap {
	doc, err := parseHTML(body)
	if err != nil {
		return DataMap{}
	}
	labels := buildLabelValueMap(doc)
	m := DataMap{}

	setIfNotEmpty(m, "upsStatus", labels["UPS Status"])
	setIfNotEmpty(m, "acStatus", labels["AC Status"])
	setNumber(m, "inputVoltage", labels["Input Line Voltage"], -1)
	setNumber(m, "inputMaxVoltage", labels["Input Max. Line Voltage"], -1)
	setNumber(m, "inputMinVoltage", labels["Input Min. Line Voltage"], -1)
	setNumber(m, "inputFrequency", labels["Input Frequency"], 1)
	setNumber(m, "outputVoltage", labels["Output Voltage"], -1)
	setIfNotEmpty(m, "outputStatus", labels["Output Status"])
	setNumber(m, "upsLoadPercentage", labels["UPS load"], -1)
	setNumber(m, "temperature", labels["Temperature"], 1)
	setIfNotEmpty(m, "batteryStatus", labels["Battery Status"])
	setNumber(m, "batteryCapacityPercentage", labels["Battery Capacity"], -1)
	setNumber(m, "batteryVoltage", labels["Battery Voltage"], 2)
	setDuration(m, "timeOnBatterySeconds", labels["Time on Battery"])
	setDuration(m, "estimatedTimeRemainingSeconds", labels["Estimated Battery Remaining Time"])
	setIfNotEmpty(m, "upsLastSelfTest", labels["UPS Last Self Test"])
	setIfNotEmpty(m, "upsNextSelfTest", labels["UPS Next Self Test"])

	return m
}

func parseSystemStatusPage(body string, cfg Config) DataMap {
	doc, err := parseHTML(body)
	if err != nil {
		return DataMap{}
	}
	labels := buildLabelValueMap(doc)
	m := DataMap{}

	setIfNotEmpty(m, "hardwareVersion", labels["Hardware Version"])
	setIfNotEmpty(m, "systemFirmwareVersion", labels["Firmware Version"])
	setIfNotEmpty(m, "serialNumber", labels["Serial Number"])
	setIfNotEmpty(m, "systemName", labels["System Name"])
	setIfNotEmpty(m, "location", labels["Location"])

	// System time from multiple sources
	sysTimeDisplay := ""
	if el := findElementByID(doc, "sys_time"); el != nil {
		sysTimeDisplay = sanitizeWhitespace(getTextContent(el))
	}
	sysTimeHidden := ""
	if el := findInputByName(doc, "$year_date_time"); el != nil {
		sysTimeHidden = getAttr(el, "value")
	}

	sysTime := sysTimeDisplay
	if sysTime == "" {
		sysTime = sysTimeHidden
	}
	if sysTime == "" {
		sysTime = labels["System Time"]
	}
	if ts := toIsoTimestamp(sysTime); ts != "" {
		m["systemTime"] = ts
	}

	// Uptime
	uptimeHidden := ""
	if el := findInputByName(doc, "$up_time_hidden"); el != nil {
		uptimeHidden = getAttr(el, "value")
	}
	if uptimeHidden != "" {
		if n, err := strconv.Atoi(uptimeHidden); err == nil {
			m["uptimeSeconds"] = n
		}
	} else {
		setDuration(m, "uptimeSeconds", labels["Uptime"])
	}

	setIfNotEmpty(m, "upsLastSelfTest", labels["UPS Last Self Test"])
	setIfNotEmpty(m, "upsNextSelfTest", labels["UPS Next Self Test"])
	setIfNotEmpty(m, "macAddress", labels["MAC Address"])

	ip := labels["IP Address"]
	if ip == "" {
		ip = cfg.UPSIP
	}
	m["ipAddress"] = ip

	setIfNotEmpty(m, "emailServer", labels["Email Server"])
	setIfNotEmpty(m, "primaryDns", labels["Primary DNS Server"])
	setIfNotEmpty(m, "secondaryDns", labels["Secondary DNS Server"])
	setIfNotEmpty(m, "pppoeIp", labels["PPPoE IP"])

	return m
}

func parseUpsInfoPage(body string) DataMap {
	doc, err := parseHTML(body)
	if err != nil {
		return DataMap{}
	}
	labels := buildLabelValueMap(doc)
	m := DataMap{}

	setIfNotEmpty(m, "upsManufacturer", labels["UPS Manufacturer"])
	setIfNotEmpty(m, "upsFirmwareVersion", labels["UPS Firmware Version"])
	setIfNotEmpty(m, "upsModel", labels["UPS Model"])
	setIfNotEmpty(m, "batteryReplacementDate", labels["Date of last battery replacement"])
	setNumber(m, "batteryCount", labels["Number of Batteries"], -1)
	setNumber(m, "batteryChargeVoltage", labels["Battery Charge Voltage"], -1)
	setNumber(m, "batteryVoltageRating", labels["Battery Voltage Rating"], -1)

	return m
}

// --- Sensor definitions ---

type SensorDef struct {
	Name               string
	Units              string
	DeviceClass        string
	StateClass         string
	EntityCategory     string
	Icon               string
	SuggestedPrecision int // -1 means not set
	TopicSuffix        string
}

var sensorDefs = map[string]SensorDef{
	"upsStatus":                     {Name: "UPS Status", Icon: "mdi:power-plug-battery", TopicSuffix: "status/general/ups_status"},
	"acStatus":                      {Name: "AC Status", Icon: "mdi:connection", TopicSuffix: "status/general/ac_status"},
	"inputVoltage":                  {Units: "V", DeviceClass: "voltage", Name: "Input Voltage", StateClass: "measurement", TopicSuffix: "status/input/line_voltage"},
	"inputMaxVoltage":               {Units: "V", DeviceClass: "voltage", Name: "Input Max Voltage", StateClass: "measurement", EntityCategory: "diagnostic", TopicSuffix: "status/input/max_voltage"},
	"inputMinVoltage":               {Units: "V", DeviceClass: "voltage", Name: "Input Min Voltage", StateClass: "measurement", EntityCategory: "diagnostic", TopicSuffix: "status/input/min_voltage"},
	"inputFrequency":                {Units: "Hz", DeviceClass: "frequency", Name: "Input Frequency", StateClass: "measurement", SuggestedPrecision: 1, TopicSuffix: "status/input/frequency"},
	"outputVoltage":                 {Units: "V", DeviceClass: "voltage", Name: "Output Voltage", StateClass: "measurement", EntityCategory: "diagnostic", TopicSuffix: "status/output/voltage"},
	"outputStatus":                  {Name: "Output Status", EntityCategory: "diagnostic", TopicSuffix: "status/output/status"},
	"upsLoadPercentage":             {Units: "%", Name: "UPS Load", StateClass: "measurement", Icon: "mdi:gauge", EntityCategory: "diagnostic", TopicSuffix: "status/output/load_percentage"},
	"temperature":                   {Units: "°C", DeviceClass: "temperature", Name: "Temperature", StateClass: "measurement", EntityCategory: "diagnostic", TopicSuffix: "status/battery/temperature"},
	"batteryStatus":                 {Name: "Battery Status", EntityCategory: "diagnostic", TopicSuffix: "status/battery/status"},
	"batteryCapacityPercentage":     {Units: "%", DeviceClass: "battery", Name: "Battery Capacity", StateClass: "measurement", Icon: "mdi:battery", TopicSuffix: "status/battery/capacity_percentage"},
	"batteryVoltage":                {Units: "V", DeviceClass: "voltage", Name: "Battery Voltage", StateClass: "measurement", SuggestedPrecision: 2, TopicSuffix: "status/battery/voltage"},
	"timeOnBatterySeconds":          {Units: "s", DeviceClass: "duration", Name: "Time on Battery", StateClass: "total_increasing", EntityCategory: "diagnostic", TopicSuffix: "status/battery/time_on_battery_seconds"},
	"estimatedTimeRemainingSeconds": {Units: "s", DeviceClass: "duration", Name: "Estimated Time Remaining", EntityCategory: "diagnostic", TopicSuffix: "status/battery/estimated_time_remaining_seconds"},
	"hardwareVersion":               {Name: "Hardware Version", EntityCategory: "diagnostic", TopicSuffix: "system/info/hardware_version"},
	"systemFirmwareVersion":         {Name: "System Firmware Version", EntityCategory: "diagnostic", TopicSuffix: "system/info/system_firmware_version"},
	"serialNumber":                  {Name: "Serial Number", EntityCategory: "diagnostic", TopicSuffix: "system/info/serial_number"},
	"systemName":                    {Name: "System Name", EntityCategory: "diagnostic", TopicSuffix: "system/info/system_name"},
	"location":                      {Name: "Location", EntityCategory: "diagnostic", TopicSuffix: "system/info/location"},
	"systemTime":                    {Name: "System Time", EntityCategory: "diagnostic", TopicSuffix: "system/info/system_time"},
	"uptimeSeconds":                 {Units: "s", DeviceClass: "duration", Name: "UPS Uptime", StateClass: "total_increasing", EntityCategory: "diagnostic", TopicSuffix: "system/info/uptime_seconds"},
	"upsLastSelfTest":               {Name: "UPS Last Self Test", EntityCategory: "diagnostic", TopicSuffix: "status/self_test/last"},
	"upsNextSelfTest":               {Name: "UPS Next Self Test", EntityCategory: "diagnostic", TopicSuffix: "status/self_test/next"},
	"macAddress":                    {Name: "MAC Address", EntityCategory: "diagnostic", TopicSuffix: "system/network/mac"},
	"ipAddress":                     {Name: "IP Address", EntityCategory: "diagnostic", TopicSuffix: "system/network/ip"},
	"emailServer":                   {Name: "Email Server", EntityCategory: "diagnostic", TopicSuffix: "system/network/email_server"},
	"primaryDns":                    {Name: "Primary DNS", EntityCategory: "diagnostic", TopicSuffix: "system/network/primary_dns"},
	"secondaryDns":                  {Name: "Secondary DNS", EntityCategory: "diagnostic", TopicSuffix: "system/network/secondary_dns"},
	"pppoeIp":                       {Name: "PPPoE IP", EntityCategory: "diagnostic", TopicSuffix: "system/network/pppoe_ip"},
	"upsManufacturer":               {Name: "UPS Manufacturer", EntityCategory: "diagnostic", TopicSuffix: "device/info/manufacturer"},
	"upsFirmwareVersion":            {Name: "UPS Firmware Version", EntityCategory: "diagnostic", TopicSuffix: "device/info/ups_firmware_version"},
	"upsModel":                      {Name: "UPS Model", EntityCategory: "diagnostic", TopicSuffix: "device/info/model"},
	"batteryReplacementDate":        {Name: "Battery Replacement Date", EntityCategory: "diagnostic", TopicSuffix: "device/battery/replacement_date"},
	"batteryCount":                  {Units: "pcs", Name: "Battery Count", EntityCategory: "diagnostic", TopicSuffix: "device/battery/count"},
	"batteryVoltageRating":          {Units: "V", DeviceClass: "voltage", Name: "Battery Voltage Rating", EntityCategory: "diagnostic", TopicSuffix: "device/battery/voltage_rating"},
}

// --- camelCase to snake_case ---

func toSnakeCase(s string) string {
	var result []rune
	for i, r := range s {
		if unicode.IsUpper(r) && i > 0 {
			result = append(result, '_')
		}
		result = append(result, unicode.ToLower(r))
	}
	out := string(result)
	out = strings.ReplaceAll(out, " ", "_")
	out = strings.ReplaceAll(out, "-", "_")
	return out
}

// --- MQTT message building ---

type MQTTMsg struct {
	Topic   string
	Payload string
	Retain  bool
}

func buildStateTopic(cfg Config, key string) string {
	def, ok := sensorDefs[key]
	if !ok {
		return ""
	}
	suffix := def.TopicSuffix
	if suffix == "" {
		suffix = toSnakeCase(key)
	}
	return cfg.UPSTopic + "/" + suffix
}

func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == math.Trunc(val) {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case int:
		return strconv.Itoa(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func prepareStateMessages(cfg Config, data DataMap) []MQTTMsg {
	var msgs []MQTTMsg
	for key, val := range data {
		topic := buildStateTopic(cfg, key)
		if topic == "" {
			continue
		}
		msgs = append(msgs, MQTTMsg{
			Topic:   topic,
			Payload: formatValue(val),
			Retain:  true,
		})
	}
	return msgs
}

func buildMqttDeviceInfo(cfg Config, systemInfo, upsInfo DataMap) map[string]interface{} {
	device := map[string]interface{}{
		"identifiers":       []string{cfg.HADeviceID},
		"configuration_url": cfg.ConfigurationURL,
	}

	name := cfg.HADeviceName
	if name == "" {
		if sn, ok := systemInfo["systemName"].(string); ok {
			name = sn
		}
	}
	if name != "" {
		device["name"] = name
	}

	if v, ok := upsInfo["upsManufacturer"].(string); ok {
		device["manufacturer"] = v
	}
	if v, ok := upsInfo["upsModel"].(string); ok {
		device["model"] = v
	}
	fw := ""
	if v, ok := upsInfo["upsFirmwareVersion"].(string); ok {
		fw = v
	}
	if fw == "" {
		if v, ok := systemInfo["systemFirmwareVersion"].(string); ok {
			fw = v
		}
	}
	if fw != "" {
		device["sw_version"] = fw
	}
	if v, ok := systemInfo["hardwareVersion"].(string); ok {
		device["hw_version"] = v
	}
	if v, ok := systemInfo["serialNumber"].(string); ok {
		device["serial_number"] = v
	}
	if v, ok := systemInfo["location"].(string); ok {
		device["suggested_area"] = v
	}
	if v, ok := systemInfo["macAddress"].(string); ok {
		device["connections"] = [][]string{{"mac", strings.ToLower(v)}}
	}

	return device
}

func prepareDiscoveryMessages(cfg Config, data DataMap, deviceInfo map[string]interface{}) []MQTTMsg {
	// Collect keys that have sensor definitions, sort by snake_case name
	type entry struct {
		key      string
		snakeKey string
	}
	var entries []entry
	for key := range data {
		if _, ok := sensorDefs[key]; ok {
			entries = append(entries, entry{key, toSnakeCase(key)})
		}
	}
	// Sort
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].snakeKey > entries[j].snakeKey {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	var msgs []MQTTMsg
	for _, e := range entries {
		def := sensorDefs[e.key]
		topic := buildStateTopic(cfg, e.key)

		payload := map[string]interface{}{
			"name":        def.Name,
			"state_topic": topic,
			"unique_id":   cfg.HADeviceID + "_" + e.snakeKey,
			"device":      deviceInfo,
		}
		if def.Units != "" {
			payload["unit_of_measurement"] = def.Units
		}
		if def.DeviceClass != "" {
			payload["device_class"] = def.DeviceClass
		}
		if def.StateClass != "" {
			payload["state_class"] = def.StateClass
		}
		if def.EntityCategory != "" {
			payload["entity_category"] = def.EntityCategory
		}
		if def.Icon != "" {
			payload["icon"] = def.Icon
		}
		if def.SuggestedPrecision > 0 {
			payload["suggested_display_precision"] = def.SuggestedPrecision
		}

		jsonBytes, err := json.Marshal(payload)
		if err != nil {
			continue
		}
		msgs = append(msgs, MQTTMsg{
			Topic:   fmt.Sprintf("%s/%s/config", cfg.DiscoveryTopicPrefix, e.snakeKey),
			Payload: string(jsonBytes),
			Retain:  true,
		})
	}
	return msgs
}

func removeGlitchyValues(data DataMap) DataMap {
	result := DataMap{}
	for k, v := range data {
		if k == "batteryVoltage" {
			if n, ok := v.(float64); ok && n < 10 {
				continue
			}
		}
		if k == "batteryCapacityPercentage" {
			if n, ok := v.(float64); ok && n < 5 {
				continue
			}
		}
		result[k] = v
	}
	return result
}

// --- MQTT publishing ---

func parseMQTTBrokerURL(raw string) string {
	// paho expects tcp:// not mqtt://
	raw = strings.Replace(raw, "mqtt://", "tcp://", 1)
	raw = strings.Replace(raw, "mqtts://", "ssl://", 1)
	return raw
}

func publishMqttMessages(cfg Config, msgs []MQTTMsg) error {
	if len(msgs) == 0 {
		return nil
	}

	opts := mqtt.NewClientOptions().
		AddBroker(parseMQTTBrokerURL(cfg.MQTTServer)).
		SetConnectTimeout(10 * time.Second).
		SetAutoReconnect(false)

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("mqtt connect timeout")
	}
	if token.Error() != nil {
		return fmt.Errorf("mqtt connect: %w", token.Error())
	}
	defer client.Disconnect(1000)

	for _, msg := range msgs {
		t := client.Publish(msg.Topic, 1, msg.Retain, msg.Payload)
		if !t.WaitTimeout(5 * time.Second) {
			return fmt.Errorf("mqtt publish timeout on %s", msg.Topic)
		}
		if t.Error() != nil {
			return fmt.Errorf("mqtt publish %s: %w", msg.Topic, t.Error())
		}
	}
	return nil
}

// --- Poll cycle ---

func pollOnce(cfg Config) (int, error) {
	statusHTML, err := fetchUpsPage(cfg, cfg.StatusPath)
	if err != nil {
		return 0, fmt.Errorf("fetch status: %w", err)
	}
	time.Sleep(500 * time.Millisecond)
	systemHTML, err := fetchUpsPage(cfg, cfg.SystemPath)
	if err != nil {
		return 0, fmt.Errorf("fetch system: %w", err)
	}
	time.Sleep(500 * time.Millisecond)
	infoHTML, err := fetchUpsPage(cfg, cfg.InfoPath)
	if err != nil {
		return 0, fmt.Errorf("fetch info: %w", err)
	}

	statusInfo := parseStatusPage(statusHTML)
	systemInfo := parseSystemStatusPage(systemHTML, cfg)
	upsInfo := parseUpsInfoPage(infoHTML)
	deviceInfo := buildMqttDeviceInfo(cfg, systemInfo, upsInfo)

	// Merge: upsInfo, then systemInfo, then statusInfo (later wins)
	combined := DataMap{}
	for k, v := range upsInfo {
		combined[k] = v
	}
	for k, v := range systemInfo {
		combined[k] = v
	}
	for k, v := range statusInfo {
		combined[k] = v
	}
	if _, ok := combined["ipAddress"]; !ok {
		combined["ipAddress"] = cfg.UPSIP
	}

	discoveryMsgs := prepareDiscoveryMessages(cfg, combined, deviceInfo)
	noGlitch := removeGlitchyValues(combined)
	stateMsgs := prepareStateMessages(cfg, noGlitch)

	allMsgs := append(stateMsgs, discoveryMsgs...)
	if err := publishMqttMessages(cfg, allMsgs); err != nil {
		return 0, err
	}
	return len(allMsgs), nil
}

// --- Main ---

func main() {
	cfg := loadConfig()

	cfgJSON, _ := json.Marshal(map[string]interface{}{
		"mqttServer":           cfg.MQTTServer,
		"upsIp":               cfg.UPSIP,
		"upsPort":             cfg.UPSHTTPPort,
		"upsTimeoutMs":        cfg.UPSHTTPTimeoutMS,
		"pollIntervalMs":      cfg.PollIntervalMS,
		"upsTopic":            cfg.UPSTopic,
		"discoveryTopicPrefix": cfg.DiscoveryTopicPrefix,
		"statusPath":          cfg.StatusPath,
		"systemPath":          cfg.SystemPath,
		"infoPath":            cfg.InfoPath,
	})
	log.Printf("[ups-poller] starting with configuration: %s", cfgJSON)
	if cfg.Debug {
		log.Println("[ups-poller] debug logging enabled")
	}

	var polling sync.Mutex
	interval := time.Duration(cfg.PollIntervalMS) * time.Millisecond

	executePoll := func() {
		if !polling.TryLock() {
			log.Println("Previous poll still running, skipping this interval")
			return
		}
		defer polling.Unlock()

		n, err := pollOnce(cfg)
		if err != nil {
			log.Printf("Failed to update UPS data: %v", err)
		} else if cfg.Debug {
			log.Printf("[ups-poller] poll ok, published %d messages", n)
		}
	}

	executePoll()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		executePoll()
	}
}
