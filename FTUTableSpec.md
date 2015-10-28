
# FxOS FTU rollups

Here is a description of the rollup table generated from the FxOS FTU data. 
Rows are basically flattened FTU records with sanitization applied to certain values, together with some useful summaries mapped from them. 
The `count` field gives the number of FTU records sharing the values listed in the row.
This table should be a sufficient data source for the majority of analysis and reporting to be run on FTU data.

Currently this table is generated as a CSV snapshot that is updated daily, and this is in the process of migrating to Redshift.

Some columns are mapped directly from the others for convenience, eg. converting country codes to country names. These can be added to the table after the fact.


## Column descriptions

Column types are either 'raw' when the raw payload value is reported as-is, 'sanitized' when a sanitization or grouping is applied to the raw value, or 'mapped' when the value can be computed from other column values.

Note that any of these column values can be missing for a given FTU record.


Column | Type | Raw field name | Details
-------|------|----------------|------------
`ping_date` | sanitized | `pingTime` | The date on which the ping was sent, as recorded by the client. It is considered less reliable than `submission_date` because of client-side clock skew. The raw value is a millisecond timestamp which is converted to a standard-format date.
`submission_date` | sanitized | | The server-side timestamp applied when the ping was received. It is the preferred way to identify "the date on which the ping was sent". The raw value is converted to a standard-format date.
`os` | sanitized | `deviceinfo.os` | The OS version number along with a tag for prerelease builds. It is reported as "m.n". Tarako devices are identified using device model and tagged as "1.3T".
`country` | mapped | `info.geoCountry` | The name of the country, mapped from the geoIP country code. Currently the country code itself is not retained.
`device` | sanitized | `deviceinfo.product_model` | The name of the device model, formatted as appropriate. Many devices report small variations in capitalization or spacing on the standard model name. These are formatted to a single common standard name using regexes, so that they are counted as the same device model.
`locale` | raw | `locale` | The locale code identifying the OS language.
`language` | mapped | | The name of the OS language, mapped from the locale code.
`update_channel` | sanitized | `app.update.channel` or `deviceinfo.update_channel` | The name of the update channel the OS build is associated with. This is recorded under two possible raw field names, although at most one appears in any given record.
`update_channel_standardized` | mapped | | The standard channel name, if the OS is on one of the Mozilla standard channels. Otherwise, "other".
`platform_version` | raw | `deviceinfo.platform_version` | The Gecko platform version.
`platform_build_id` | raw | `deviceinfo.platform_build_id` | The Gecko platform build ID (includes a timestamp of when the build was compiled).
`sim_mcc` | raw | `icc.mcc` | The mobile country code read from the SIM card.
`sim_mnc` | raw | `icc.mnc` | The mobile network code read from the SIM card.
`sim_mcc_country` | mapped | | The name of the country identified by the SIM MCC.
`sim_mnc_network` | mapped | | The name of the network operator identified by the SIM MCC and MNC.
`sim_network_name` | sanitized | `icc.spn` | The service provider name read from the SIM card, formatted for consistency.
`network_mcc` | raw | `network.mcc` | The mobile country code read from the currently connected mobile network.
`network_mnc` | raw | `network.mnc` | The mobile network code read from the currently connected mobile network.
`network_mcc_country` | mapped | | The name of the country identified by the network MCC.
`network_mnc_network` | mapped | | The name of the network operator identified by the network MCC and MNC.
`network_network_name` | sanitized | `network.operator` | The network operator name read from the network, formatted for consistency.
`screen_width` | raw | `screenWidth` | The width of the screen in pixels.
`screen_height` | raw | `screenHeight` | The height of the screen in pixels.
`device_pixel_ratio` | raw | `devicePixelRatio` | The ratio of physical to logical pixels.
`software` | raw | `deviceinfo.software` | The name of the OS distribution.
`hardware` | raw | `deviceinfo.hardware` | String identifying the hardware manufacturer and chipset.
`firmware_revision` | raw | `deviceinfo.firmware_revision` | String identifying the firmware revision version.
`activation_date` | sanitized | `activationTime` | The date on which the device was activated, as recorded by the client. This value is very unreliable because of client-side clock skew. On many devices, it is recorded before the local time has been set, resulting in values close to the 1970-01-01 origin. The raw value is a millisecond timestamp which is converted to a standard-format date.



