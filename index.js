require('dotenv').config();

const mqtt = require('mqtt');
const { JSDOM } = require('jsdom');
const net = require('net');

const MQTT_SERVER = process.env.MQTT_SERVER || 'mqtt://192.168.1.1';
const IP_ADDRESS = process.env.UPS_IP || '192.168.1.2';
const UPS_HTTP_PORT = Number(process.env.UPS_HTTP_PORT || 80);
const UPS_HTTP_TIMEOUT_MS = Number(process.env.UPS_HTTP_TIMEOUT_MS || 5000);

const UPS_TOPIC = process.env.UPS_TOPIC || 'ups-netagent';
const DISCOVERY_TOPIC_PREFIX = process.env.DISCOVERY_TOPIC_PREFIX || `homeassistant/sensor/${UPS_TOPIC}`;
const UPS_PAGE_PATHS = {
    status: process.env.UPS_STATUS_PATH || '/pda/status-1.htm',
    system: process.env.UPS_SYSTEM_PATH || '/pda/sys_status.htm',
    info: process.env.UPS_INFO_PATH || '/pda/UPS.htm'
};
const HA_DEVICE_ID = process.env.HA_DEVICE_ID || 'ups_netagent';
const HA_DEVICE_NAME = process.env.HA_DEVICE_NAME;

const CONFIGURATION_URL = process.env.UPS_CONFIG_URL || (UPS_HTTP_PORT === 80 ? `http://${IP_ADDRESS}` : `http://${IP_ADDRESS}:${UPS_HTTP_PORT}`);
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_SECONDS || 20) * 1000;

const logStartupConfiguration = () => {
    console.info('[ups-poller] starting with configuration:', JSON.stringify({
        mqttServer: MQTT_SERVER,
        upsIp: IP_ADDRESS,
        upsPort: UPS_HTTP_PORT,
        upsTimeoutMs: UPS_HTTP_TIMEOUT_MS,
        pollIntervalMs: POLL_INTERVAL_MS,
        upsTopic: UPS_TOPIC,
        discoveryTopicPrefix: DISCOVERY_TOPIC_PREFIX,
        statusPath: UPS_PAGE_PATHS.status,
        systemPath: UPS_PAGE_PATHS.system,
        infoPath: UPS_PAGE_PATHS.info
    }));
};


const fetchUpsPage = (path) => {
    return new Promise((resolve, reject) => {
        const client = net.createConnection({ host: IP_ADDRESS, port: UPS_HTTP_PORT }, () => {
            const headers = [
                `GET ${path} HTTP/1.1`,
                `Host: ${IP_ADDRESS}`,
                'Connection: close',
                '',
                ''
            ].join('\r\n');
            client.write(headers);
        });

        const chunks = [];
        let settled = false;

        const finish = (err, body) => {
            if (settled) {
                return;
            }
            settled = true;
            if (err) {
                reject(err);
            } else {
                resolve(body);
            }
        };

        client.setTimeout(UPS_HTTP_TIMEOUT_MS);

        client.on('data', (chunk) => {
            chunks.push(chunk);
        });

        client.on('end', () => {
            const rawResponse = Buffer.concat(chunks).toString();
            const separatorIndex = rawResponse.indexOf('\r\n\r\n');
            if (separatorIndex === -1) {
                finish(new Error('Invalid HTTP response from UPS'));
                return;
            }

            const body = rawResponse.substring(separatorIndex + 4);
            finish(null, body);
        });

        client.on('error', (err) => {
            finish(err);
        });

        client.on('timeout', () => {
            client.destroy(new Error('UPS request timed out'));
        });

        client.on('close', () => {
            // no-op; connection closed after data retrieval
        });
    });
};

const sanitizeWhitespace = (value = '') => value.replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();

const collectValueAfterLabel = (boldEl) => {
    let node = boldEl.nextSibling;
    const parts = [];

    while (node) {
        if (node.nodeName === 'BR') {
            break;
        }

        if (node.nodeType === 3 && node.textContent) {
            parts.push(node.textContent);
        }

        if (node.nodeType === 1) {
            if (node.tagName === 'INPUT') {
                // Ignore hidden inputs when extracting text values.
            } else {
                parts.push(node.textContent);
            }
        }

        node = node.nextSibling;
    }

    const value = sanitizeWhitespace(parts.join(' '));
    return value.length ? value : undefined;
};

const buildLabelValueMap = (document) => {
    const labels = {};
    document.querySelectorAll('b').forEach((boldEl) => {
        const labelText = sanitizeWhitespace(boldEl.textContent).replace(/:$/, '');
        if (!labelText) {
            return;
        }

        const value = collectValueAfterLabel(boldEl);
        if (value !== undefined) {
            labels[labelText] = value;
        }
    });

    return labels;
};

const extractFirstNumber = (value, precision) => {
    if (value === undefined) {
        return undefined;
    }

    const match = value.match(/-?\d+(?:\.\d+)?/);
    if (!match) {
        return undefined;
    }

    const numberValue = Number(match[0]);
    if (typeof precision === 'number') {
        const factor = Math.pow(10, precision);
        return Math.round(numberValue * factor) / factor;
    }

    return numberValue;
};

const parseDurationToSeconds = (value) => {
    if (!value || value.includes('--')) {
        return undefined;
    }

    const parts = value.split(':');
    if (parts.length !== 3) {
        return undefined;
    }

    return parts.reduce((acc, part) => (acc * 60) + Number(part), 0);
};

const toIsoTimestamp = (value) => {
    if (!value) {
        return undefined;
    }

    const trimmed = value.trim();
    if (!trimmed || trimmed === '--') {
        return undefined;
    }

    return trimmed.replace(/\//g, '-');
};

const filterEmptyValues = (data) => {
    return Object.entries(data).reduce((acc, [key, value]) => {
        if (value === undefined || value === null) {
            return acc;
        }
        if (typeof value === 'string' && value.trim() === '') {
            return acc;
        }
        if (typeof value === 'number' && Number.isNaN(value)) {
            return acc;
        }
        acc[key] = value;
        return acc;
    }, {});
};

const parseStatusPage = (html) => {
    const dom = new JSDOM(html);
    const document = dom.window.document;
    const labels = buildLabelValueMap(document);

    return filterEmptyValues({
        upsStatus: labels['UPS Status'],
        acStatus: labels['AC Status'],
        inputVoltage: extractFirstNumber(labels['Input Line Voltage']),
        inputMaxVoltage: extractFirstNumber(labels['Input Max. Line Voltage']),
        inputMinVoltage: extractFirstNumber(labels['Input Min. Line Voltage']),
        inputFrequency: extractFirstNumber(labels['Input Frequency'], 1),
        outputVoltage: extractFirstNumber(labels['Output Voltage']),
        outputStatus: labels['Output Status'],
        upsLoadPercentage: extractFirstNumber(labels['UPS load']),
        temperature: extractFirstNumber(labels['Temperature'], 1),
        batteryStatus: labels['Battery Status'],
        batteryCapacityPercentage: extractFirstNumber(labels['Battery Capacity']),
        batteryVoltage: extractFirstNumber(labels['Battery Voltage'], 2),
        timeOnBatterySeconds: parseDurationToSeconds(labels['Time on Battery']),
        estimatedTimeRemainingSeconds: parseDurationToSeconds(labels['Estimated Battery Remaining Time']),
        upsLastSelfTest: labels['UPS Last Self Test'],
        upsNextSelfTest: labels['UPS Next Self Test']
    });
};

const parseSystemStatusPage = (html) => {
    const dom = new JSDOM(html);
    const document = dom.window.document;
    const labels = buildLabelValueMap(document);
    const systemTimeDisplay = sanitizeWhitespace(document.querySelector('#sys_time')?.textContent || '');
    const systemTimeHidden = document.querySelector('input[name="$year_date_time"]')?.getAttribute('value');
    const uptimeSecondsHidden = document.querySelector('input[name="$up_time_hidden"]')?.getAttribute('value');

    return filterEmptyValues({
        hardwareVersion: labels['Hardware Version'],
        systemFirmwareVersion: labels['Firmware Version'],
        serialNumber: labels['Serial Number'],
        systemName: labels['System Name'],
        location: labels['Location'],
        systemTime: toIsoTimestamp(systemTimeDisplay || systemTimeHidden || labels['System Time']),
        uptimeSeconds: uptimeSecondsHidden ? Number(uptimeSecondsHidden) : parseDurationToSeconds(labels['Uptime']),
        upsLastSelfTest: labels['UPS Last Self Test'],
        upsNextSelfTest: labels['UPS Next Self Test'],
        macAddress: labels['MAC Address'],
        ipAddress: labels['IP Address'] || IP_ADDRESS,
        emailServer: labels['Email Server'],
        primaryDns: labels['Primary DNS Server'],
        secondaryDns: labels['Secondary DNS Server'],
        pppoeIp: labels['PPPoE IP']
    });
};

const parseUpsInfoPage = (html) => {
    const dom = new JSDOM(html);
    const document = dom.window.document;
    const labels = buildLabelValueMap(document);

    return filterEmptyValues({
        upsManufacturer: labels['UPS Manufacturer'],
        upsFirmwareVersion: labels['UPS Firmware Version'],
        upsModel: labels['UPS Model'],
        batteryReplacementDate: labels['Date of last battery replacement'],
        batteryCount: extractFirstNumber(labels['Number of Batteries']),
        batteryChargeVoltage: extractFirstNumber(labels['Battery Charge Voltage']),
        batteryVoltageRating: extractFirstNumber(labels['Battery Voltage Rating'])
    });
};

const buildMqttDeviceInfo = (systemInfo, upsInfo) => {
    const device = {
        identifiers: [HA_DEVICE_ID],
        name: HA_DEVICE_NAME || systemInfo.systemName,
        configuration_url: CONFIGURATION_URL
    };

    if (upsInfo.upsManufacturer) {
        device.manufacturer = upsInfo.upsManufacturer;
    }

    if (upsInfo.upsModel) {
        device.model = upsInfo.upsModel;
    }

    if (upsInfo.upsFirmwareVersion || systemInfo.systemFirmwareVersion) {
        device.sw_version = upsInfo.upsFirmwareVersion || systemInfo.systemFirmwareVersion;
    }

    if (systemInfo.hardwareVersion) {
        device.hw_version = systemInfo.hardwareVersion;
    }

    if (systemInfo.serialNumber) {
        device.serial_number = systemInfo.serialNumber;
    }

    if (systemInfo.location) {
        device.suggested_area = systemInfo.location;
    }

    if (systemInfo.macAddress) {
        device.connections = [['mac', systemInfo.macAddress.toLowerCase()]];
    }

    return device;
};

const SENSOR_DEFINITIONS = {
    upsStatus: { name: 'UPS Status', icon: 'mdi:power-plug-battery', topicSuffix: 'status/general/ups_status' },
    acStatus: { name: 'AC Status', icon: 'mdi:connection', topicSuffix: 'status/general/ac_status' },
    inputVoltage: { units: 'V', deviceClass: 'voltage', name: 'Input Voltage', stateClass: 'measurement', topicSuffix: 'status/input/line_voltage' },
    inputMaxVoltage: { units: 'V', deviceClass: 'voltage', name: 'Input Max Voltage', stateClass: 'measurement', entityCategory: 'diagnostic', topicSuffix: 'status/input/max_voltage' },
    inputMinVoltage: { units: 'V', deviceClass: 'voltage', name: 'Input Min Voltage', stateClass: 'measurement', entityCategory: 'diagnostic', topicSuffix: 'status/input/min_voltage' },
    inputFrequency: { units: 'Hz', deviceClass: 'frequency', name: 'Input Frequency', stateClass: 'measurement', suggestedPrecision: 1, topicSuffix: 'status/input/frequency' },
    outputVoltage: { units: 'V', deviceClass: 'voltage', name: 'Output Voltage', stateClass: 'measurement', entityCategory: 'diagnostic', topicSuffix: 'status/output/voltage' },
    outputStatus: { name: 'Output Status', entityCategory: 'diagnostic', topicSuffix: 'status/output/status' },
    upsLoadPercentage: { units: '%', name: 'UPS Load', stateClass: 'measurement', icon: 'mdi:gauge', entityCategory: 'diagnostic', topicSuffix: 'status/output/load_percentage' },
    temperature: { units: '°C', deviceClass: 'temperature', name: 'Temperature', stateClass: 'measurement', entityCategory: 'diagnostic', topicSuffix: 'status/battery/temperature' },
    batteryStatus: { name: 'Battery Status', entityCategory: 'diagnostic', topicSuffix: 'status/battery/status' },
    batteryCapacityPercentage: { units: '%', deviceClass: 'battery', name: 'Battery Capacity', stateClass: 'measurement', icon: 'mdi:battery', topicSuffix: 'status/battery/capacity_percentage' },
    batteryVoltage: { units: 'V', deviceClass: 'voltage', name: 'Battery Voltage', stateClass: 'measurement', suggestedPrecision: 2, topicSuffix: 'status/battery/voltage' },
    timeOnBatterySeconds: { units: 's', deviceClass: 'duration', name: 'Time on Battery', stateClass: 'total_increasing', entityCategory: 'diagnostic', topicSuffix: 'status/battery/time_on_battery_seconds' },
    estimatedTimeRemainingSeconds: { units: 's', deviceClass: 'duration', name: 'Estimated Time Remaining', entityCategory: 'diagnostic', topicSuffix: 'status/battery/estimated_time_remaining_seconds' },
    hardwareVersion: { name: 'Hardware Version', entityCategory: 'diagnostic', topicSuffix: 'system/info/hardware_version' },
    systemFirmwareVersion: { name: 'System Firmware Version', entityCategory: 'diagnostic', topicSuffix: 'system/info/system_firmware_version' },
    serialNumber: { name: 'Serial Number', entityCategory: 'diagnostic', topicSuffix: 'system/info/serial_number' },
    systemName: { name: 'System Name', entityCategory: 'diagnostic', topicSuffix: 'system/info/system_name' },
    location: { name: 'Location', entityCategory: 'diagnostic', topicSuffix: 'system/info/location' },
    systemTime: { name: 'System Time', entityCategory: 'diagnostic', topicSuffix: 'system/info/system_time' },
    uptimeSeconds: { units: 's', deviceClass: 'duration', name: 'UPS Uptime', stateClass: 'total_increasing', entityCategory: 'diagnostic', topicSuffix: 'system/info/uptime_seconds' },
    upsLastSelfTest: { name: 'UPS Last Self Test', entityCategory: 'diagnostic', topicSuffix: 'status/self_test/last' },
    upsNextSelfTest: { name: 'UPS Next Self Test', entityCategory: 'diagnostic', topicSuffix: 'status/self_test/next' },
    macAddress: { name: 'MAC Address', entityCategory: 'diagnostic', topicSuffix: 'system/network/mac' },
    ipAddress: { name: 'IP Address', entityCategory: 'diagnostic', topicSuffix: 'system/network/ip' },
    emailServer: { name: 'Email Server', entityCategory: 'diagnostic', topicSuffix: 'system/network/email_server' },
    primaryDns: { name: 'Primary DNS', entityCategory: 'diagnostic', topicSuffix: 'system/network/primary_dns' },
    secondaryDns: { name: 'Secondary DNS', entityCategory: 'diagnostic', topicSuffix: 'system/network/secondary_dns' },
    pppoeIp: { name: 'PPPoE IP', entityCategory: 'diagnostic', topicSuffix: 'system/network/pppoe_ip' },
    upsManufacturer: { name: 'UPS Manufacturer', entityCategory: 'diagnostic', topicSuffix: 'device/info/manufacturer' },
    upsFirmwareVersion: { name: 'UPS Firmware Version', entityCategory: 'diagnostic', topicSuffix: 'device/info/ups_firmware_version' },
    upsModel: { name: 'UPS Model', entityCategory: 'diagnostic', topicSuffix: 'device/info/model' },
    batteryReplacementDate: { name: 'Battery Replacement Date', entityCategory: 'diagnostic', topicSuffix: 'device/battery/replacement_date' },
    batteryCount: { units: 'pcs', name: 'Battery Count', entityCategory: 'diagnostic', topicSuffix: 'device/battery/count' },
    //batteryChargeVoltage: { units: 'V', deviceClass: 'voltage', name: 'Battery Charge Voltage', entityCategory: 'diagnostic', suggestedPrecision: 2, topicSuffix: 'device/battery/charge_voltage' },
    batteryVoltageRating: { units: 'V', deviceClass: 'voltage', name: 'Battery Voltage Rating', entityCategory: 'diagnostic', topicSuffix: 'device/battery/voltage_rating' }
};

const buildStateTopic = (key) => {
    const definition = SENSOR_DEFINITIONS[key];
    if (!definition) {
        return undefined;
    }

    return `${UPS_TOPIC}/${definition.topicSuffix || key}`;
};

const prepareStateMessages = (statusInfo) => {
    return Object.entries(statusInfo)
        .map(([key, value]) => {
            const topic = buildStateTopic(key);
            if (!topic) {
                return undefined;
            }

            return {
                topic,
                message: value,
                retain: true
            };
        })
        .filter(Boolean);
};

const prepareDiscoveryMessages = (statusInfo, deviceInfo) => {
    return Object.entries(statusInfo)
        .filter(([key]) => SENSOR_DEFINITIONS[key])
        .map(([key]) => {
            const definition = SENSOR_DEFINITIONS[key];
            const topic = buildStateTopic(key);
            return {
                topic: `${DISCOVERY_TOPIC_PREFIX}/${key}/config`,
                retain: true,
                message: {
                    name: definition.name,
                    state_topic: topic,
                    unique_id: `${HA_DEVICE_ID}_${key}`,
                    device: deviceInfo,
                    ...(definition.units && { unit_of_measurement: definition.units }),
                    ...(definition.deviceClass && { device_class: definition.deviceClass }),
                    ...(definition.stateClass && { state_class: definition.stateClass }),
                    ...(definition.entityCategory && { entity_category: definition.entityCategory }),
                    ...(definition.icon && { icon: definition.icon }),
                    ...(typeof definition.suggestedPrecision === 'number' && { suggested_display_precision: definition.suggestedPrecision })
                }
            };
        });
};

const removeGlitchyValues = (statusInfo) => {
    return Object.keys(statusInfo).reduce((acc, key) => {
        if (key === 'batteryVoltage' && typeof statusInfo[key] === 'number' && statusInfo[key] < 10) {
            return acc;
        }
        if (key === 'batteryCapacityPercentage' && typeof statusInfo[key] === 'number' && statusInfo[key] < 5) {
            return acc;
        }
        acc[key] = statusInfo[key];
        return acc;
    }, {});
};

const publishMqttMessages = (messages) => {
    return new Promise((resolve, reject) => {
        if (!messages.length) {
            resolve();
            return;
        }

        const mqttClient = mqtt.connect(MQTT_SERVER);
        let settled = false;

        const finish = (err) => {
            if (settled) {
                return;
            }
            settled = true;
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        };

        mqttClient.on('connect', () => {
            let remaining = messages.length;

            messages.forEach((msg) => {
                const payload = typeof msg.message === 'string' ? msg.message : JSON.stringify(msg.message);
                mqttClient.publish(msg.topic, payload, { qos: 1, retain: msg.retain }, (err) => {
                    if (err) {
                        mqttClient.end(true, () => finish(err));
                        return;
                    }

                    remaining -= 1;

                    if (remaining === 0) {
                        mqttClient.end(false, () => {
                            finish();
                        });
                    }
                });
            });
        });

        mqttClient.on('error', (err) => {
            mqttClient.end(true, () => finish(err));
        });
    });
};

const pollOnce = async () => {
    const [statusHtml, systemHtml, infoHtml] = await Promise.all([
        fetchUpsPage(UPS_PAGE_PATHS.status),
        fetchUpsPage(UPS_PAGE_PATHS.system),
        fetchUpsPage(UPS_PAGE_PATHS.info)
    ]);

    const statusInfo = parseStatusPage(statusHtml);
    const systemInfo = parseSystemStatusPage(systemHtml);
    const upsInfo = parseUpsInfoPage(infoHtml);
    const deviceInfo = buildMqttDeviceInfo(systemInfo, upsInfo);

    const combinedInfo = filterEmptyValues({
        ...upsInfo,
        ...systemInfo,
        ...statusInfo,
        ipAddress: systemInfo.ipAddress || IP_ADDRESS
    });

    const discoveryMsgs = prepareDiscoveryMessages(combinedInfo, deviceInfo);
    const statusInfoNoGlitches = removeGlitchyValues(combinedInfo);
    const stateMsgs = prepareStateMessages(statusInfoNoGlitches);
    const messages = [...stateMsgs, ...discoveryMsgs];

    await publishMqttMessages(messages);
};

const startPolling = async () => {
    logStartupConfiguration();
    let isPolling = false;

    const executePoll = async () => {
        if (isPolling) {
            console.warn('Previous poll still running, skipping this interval');
            return;
        }

        isPolling = true;
        try {
            await pollOnce();
        } catch (error) {
            console.error('Failed to update UPS data:', error);
        } finally {
            isPolling = false;
        }
    };

    await executePoll();
    setInterval(executePoll, POLL_INTERVAL_MS);
};

startPolling().catch((error) => {
    console.error('UPS poller stopped due to an unrecoverable error:', error);
    process.exit(1);
});
