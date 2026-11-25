# MetrANOVA ClickHouse Naming Conventions

# Introduction

## Purpose

This document outlines the naming conventions to be followed by the MetrANOVA project when creating ClickHouse database table schemas. Adhering to these conventions ensures consistency, readability, and maintainability across the project.

## Formatting in this document

The following formatting and styles are used within this document:

* All naming formats and associated examples are green monospaced text. e.g.  `interface`  
  * Note that example table and field names in this document do not necessarily represent actual table and field names unless otherwise specified (such as when discussing reserved terms). See table schema documents for actual names that apply these conventions.  
* Text in red monospaced text is an example of a naming standard violation e.g. `IFace.Id`  
* All values within formats between `<>` are placeholders that should be replaced with a real value  
* All values within `()` are optional

## Definitions

These are common terms used throughout this document when describing the naming conventions:

| Term | Definition |
| :---- | :---- |
| **Resources** | The entity being measured or described. *Examples: router, interface, flow* |
| **Attributes** | Characteristics of resources. *Examples: name, speed, source IP address* |
| **Metadata** | A collection of attributes that describe a particular resource.  |
| **Metrics** | Quantifiable measurements of a resource used to assess its performance, health and/or efficiency. *Examples: memory utilization, uptime, traffic rate* |
| **Data** | A collection of metrics associated with a resource. |

# General Naming Rules

* **Case and Separator:** All names must use lowercase letters with words separated by an underscore (i.e. snake case). Numbers are allowed but no other special characters.  
* **Singular Nouns:** All names should be singular (e.g., `flow`, `interface`, not `flows`, `interfaces`) with the exception of metric units where it is more natural to say things such as `bits` instead of `bit.`  
* **Consistent Naming:** Databases, tables, fields and associated structures should all refer to resources, attributes and metrics in consistent ways. This means using consistent terms and abbreviations when referring to the same concept.

# Database Naming Conventions

## Default Database

The default database name is `metranova.`The default should be used if there is an expectation that only one database will be needed on a given ClickHouse instance.

## Multi-Tenancy

If multiple databases need to be hosted on the same Clickhouse instance then the convention `metranova_<description>` should be used where `<description>` is replaced with a value describing the purpose of this database. Examples include:

* `metranova_customer1`  
* `metranova_customer2`  
* `metranova_dev`  
* `metranova_prod`

# Table Naming Conventions

## General Table Naming Format

Table names take one of two forms depending on whether the data is primarily descriptive attributes (i.e. metadata) or contains metrics (i.e. data):

* `meta_<resource_type>(_<aggregation_type>)(_<aggregation_window>)`  
* `data_<resource_type>(_<metric_type>)(_<aggregation_type>)(_<aggregation_window>)`

Examples Include:

* `meta_interface`  
* `meta_interface_latest`  
* `data_interface_traffic`  
* `data_interface_uptime`  
* `data_interface_traffic_5m`  
* `data_flow`  
* `data_flow_edge`  
* `data_flow_edge_by_as_5m`

Each of the components of the general naming format for tables has their own conventions that will be discussed in the sections that follow.

## Table Type Prefix

The first component of the table naming format is a prefix indicating whether a table is a metadata table or a data table. The allowed prefixes are as follows:

* `meta_` \- Indicates a metadata table containing descriptive information about a resource  
* `data_` \- Indicates a data table containing metrics for a resource

It will often be the case that a data table may also contain fields considered metadata. This may be required to identify the series or for query optimization purposes (e.g. there is often little advantage to storing high cardinality metadata separate from its associated metrics). *A good rule of thumb is that if a table contains metrics then it is a data table, otherwise it is a metadata table.* 

## Resource Type

A resource is the object being described in the case of metadata and the object being measured in the case of data. A few key considerations when determining how to name a type of resource:

* The resource used in the table name should be the one common to all the metadata or data in the table. For example, you may have metrics in a table that describe only CPU. In such a case it would be ok to label the resource type as `cpu`. If instead you found it advantageous to have CPU and memory metrics for a given host in the same table, then you should use `host` as the resource type since it is common to all the metrics.  
* Resource types should use underscores when it makes the resource more readable. A common case is when the best name for the resource type has spaces when used in common speech. e.g. `chassis_part` instead of `chassispart`  
* **For both metadata and data tables**, where there are multiple tables **with the same structure** that describe the same type of resource, the format `<primary_resource>_<subcategory>` should be used. This often happens when tables need to be separated for optimization purposes (e.g. different retention policies, reduce search space for common queries). For example, if you consider the primary resource to be a flow record but need to split flow record from the edge of the network vs the core of the network, then the respective resources should be named `flow_edge` and `flow_core`  
* **For metadata tables**, where there are multiple tables **with a different structure** that describe the same type of resource, the format `<primary_resource>_<subcategory>` should be used.  There are many cases where this may occur, such as metadata that comes from different sources or vendor-specific metadata that needs to be separated from common metadata. The resource with no `_<subcategory>` should be reserved for a common metadata table.  Example:  `interface_inventorydb`, `interface_vendor1`. *Note: For data tables, these types of distinctions should be made at the `<metric_type>` component of the name, NOT in the* `<resource_type>`*. See the next section.*

## Metric Type

The metric type is an optional component that applies only to data tables and describes the category of metrics contained within the table. A few key considerations:

* If a table contains a general set of metrics about a resource with no clear linkage beyond describing that resource, then this component can be skipped  
* If the metrics in the table form a functional category of metrics, then this should have a clear concise name that describes that fact. For example, if all the metrics relate to the uptime of a resource, then `uptime` would be a good name.This distinction exists because it may be beneficial to separate metrics about the same resource into different tables for optimization and/or organizational purposes.  
* If a functional category needs to be further divided into subcategories, you can do so by adding a subcategory. For example, let’s say a vendor has their own flavor of uptime metrics that we want to keep separate from the generic uptime metrics. In that case we could name them something like `uptime_vendor1`.

### Reserved Metric Types `counter` and `gauge`

There are reserved metric types `counter` and `gauge` which are associated with tables that have a specific format described in the table schema document. Do not use either of these in a table name unless the table has the specific format. 

## Aggregation Type

The aggregation type indicates how a metadata or data table was aggregated from the original tables. This component is applicable if the table was aggregated from another table. Aggregation in this context means aggregation beyond the original measurement received, since many measurements can be considered “aggregated” in some sense of the term before they first reach the database. Note that the name leading up to the `<aggregation_type>` should match that of the original table. The `<aggregation_type>` is comprised of any combination of the following components in the order listed (all optional):

* `<aggregation_function>` \- The aggregation function being applied to the metric fields in the aggregation. Can skip if multiple functions are being applied.   
* `<metric_category>` \- A concise description of the metrics being included in the aggregation. Can be skipped if all metrics are included in the aggregation.   
* `by_<group_by_field_category>` \- A concise description of the non-metric fields being used to group the aggregation. Can be skipped if all non-metric fields are included.  

A few key considerations when naming the `<aggregation_type>`:

* When there is a singular aggregation function being performed on the metric fields of the original table then the `<aggregation_function>` must be included in the table name. If the aggregation function is included in one of the [common aggregation functions](#aggregation-functions), then use that, otherwise use a clear, concise name (and think about suggesting it to this guide). Examples: `max,total`  
* When only a subset of metrics are being aggregated from the original, indicate this subset directly after the aggregation function using a concise name. The format is then `<function>_<metric_category>.` For example, if we have an aggregation that calculates the maximum on a field called mtu, then the aggregation type would be `max_mtu`. Another example would be if we are summing multiple metrics, but only those measuring the incoming direction, then we would name it `total_in`.  
* If there are multiple types of aggregation functions occurring on the metrics fields then don’t worry about specifying the aggregation function  
* When we are grouping by a subset of the non-metric fields, then they should be included after the `<function>` and/or `<metric_category>` (depending on if either is present based on the previous bullets) in the form `by_<group_by_field_category>`. It should always start with `by` followed by a concise description of the fields being grouped. If there are many fields, prioritize the ones most contributing to the cardinality or try to find some concise term to describe the resulting grouping. Example: `total_by_src_ip`, `by_asn`, `max_mtu_by_id`  
* The `<aggregation_type>` can be  skipped entirely if there are multiple aggregation functions applied, all metrics are being aggregated AND the only grouping is time (i.e. all other non-metric fields are preserved in the aggregation). In this case the aggregation window ([see associated section](#aggregation-window)) is required to distinguish from the base table.

## Aggregation Window {#aggregation-window}

For tables aggregated into time buckets, the aggregation window must be included in the table name. This component is only applicable if the table was aggregated from another table and is used in situations similar to `<aggregation_type>`. For example, if flow data is exported every minute by a router, it does not need to go to a table with `1m` in the name. In contrast, if that data is further aggregated into 1 hour buckets after being inserted into the database, then the new table should have `1h` in the name. 

The window should be expressed using the largest unit that allows it to be a whole number (e.g., `1d` instead of `24h`). The following abbreviations for units should be used:

| Unit | Abbreviation | Example |
| :---- | :---- | :---- |
| Seconds | `s` | `30s` (30 seconds) |
| Minutes | `m` | `5m` (5 minutes) |
| Hours | `h` | `2h` 2 hours) |
| Days | `d` | `1d` (1 day) |
| Weeks | `w` | `2w` (2 weeks) |
| Months | `mo` | `6mo` (6 months) |
| Years | `y` | `1y` (1 year) |

# Materialized View Naming Conventions

Materialized views should be named for their source table with a suffix of `mv`. If there are multiple materialized views writing to the same table, then a `distinguisher` must be added that describes the differences between the materialized views. Generally this will be an abbreviated indication of differences in the tables or fields considered for input. The naming convention in summarized by the format below:

`<source_table_name>(_<distinguisher>)_mv`

**Examples:**

* `data_flow_by_edge_as_5m_mv`  
* `meta_interface_latest_mv`  
* `data_flow_count_by_edge_as_5m_src_in_mv`  
* `data_flow_count_by_as_5m_dst_out_mv`

# View Naming Conventions

Views should be named for their source table, contain a description of the view and have a suffix of `view` as summarized by the format below:

`<source_table_name>_<view_description>_view`

The `<view_description>` should be concise and explain the purpose of the view. If the view is being used to aggregate data in place of a new aggregate view then it should follow normal aggregate table naming conventions when forming the `<view_description>`.

**Examples:**

* `data_interface_traffic_simple_view`  
* `data_flow_rate_view`

# Dictionary Naming Conventions

Dictionaries should follow general naming guidelines and always end in `_dict`. If a dictionary is loaded directly from a table then it should table the form `<source_table_name>_dict` where `<source_table_name>` is the name of the original table.

**Examples:**

* `meta_interface_dict`  
* `meta_as_dict`

# Field Naming Conventions

## General Field Naming Principles

* Fields names should be concise and reuse names that refer to the same resource, metadata attribute or metric across tables  
* Use abbreviations when in list of [common abbreviations](#common-abbreviations)  
* All fields start have an optional `<namespace>` component for grouping similar fields together. This is useful when the table represents a resource with multiple sub-resources. An example might be a host data table with metric fields for cpu and memory. In that case, metrics could have a namespace prefix of `cpu` and `memory` to indicate their respective grouping.

## Time Fields

Time fields take the format:

`(<namespace>_)<time_type>_time`

It is expected that it will have a descriptive `<time_type>` component to indicate what the time represents followed by a suffix to indicate it is a time field. See [Common Time Fields](#common-time-fields) for a list of defined types. The common names should be used if their description matches the purpose of the time field in question. It is generally not desirable to use a name like `time` since it is ambiguous as to what the time represents. 

### Common Time Fields {#common-time-fields}

* `start_time` \- If the row represents a time interval, the beginning of the interval  
* `end_time` \- If the row represents a time interval, the end of the interval  
* `observed_time` \- The best timestamp available for when the value for the metrics in the row were observed. Ideally this would be sent to the collector from the source of truth, but in reality this may be closer to `collector_in_time`. This is generally a catch-all timestamp for metrics that represent an instance in time and not an interval with a start and end.  
* `insert_time` \- The time the row was inserted into ClickHouse

## Resource IDs and Metadata Table References

Metadata tables describe resources and their attributes. Metadata tables may or may not track changes to those resources over time. With that in mind we need a field that consistently identifies a resource, even as its attributes change over time. We also need a field that links to different versions of the resource as the attributes change. The fields respectively are as follows:

* `id` \- This is an identifier that uniquely identifies the resource described by the metadata. This id will stay the same even as the attributes change over time.   
* `ref` \- This is a field associated with a particular version of the resource and can be referenced by other tables that want to link to that version.

The naming convention for the direct column approach is as follows:

* `(<namespace>_)(<directionality>_)<resource_type>_id` \- Points at the non-versioned identifier. This should be used in cases where tracking resource versions is not required or it is useful to have this identifier directly in the table. **Example:** `in_interface_id`, `out_interface_id`, `service_id`  
* `(<namespace>_)(<directionality>_)<resource_type>_ref` \- References  the versioned identifier for the resource. **Example:** `in_interface_ref`, `out_interface_ref`, `service_ref`

The naming conventions above should also be used for subcolumns when using the `ext` JSON field. The JSON structure should be flat and contain only the keys above and their values. In ClickHouse JSON fields are accessed using dot notation.  **Example:**  `ext.in_interface_id`, `ext.out_interface_id`, `ext.service_id, ext.in_interface_ref`, `ext.out_interface_ref`, `ext.service_ref`

## Metadata Attributes

Metadata attribute fields are named as follows:

`(<namespace>_)(<directionality>_)<attribute_name>`

If directionality is applicable then that goes before the attribute name. The `<attribute_name>` should be concise, descriptive and follow general naming conventions. There are no other requirements for this component. 

## Metrics

Metrics should be referred to as follows:

`(<namespace>_)(<directionality>_)<metric_name>(_<metric_units>)(_<aggregation_function>)(_<aggregation_window>)`

A few key considerations:

* The `<directionality>` field should be included if applicable, otherwise it can be omitted  
* The `<metric_name>` should be a concise description of the metric and is the only required portion.   
* The `<metric_units>` should be included if the metric has a well-defined consistent unit. Use the [common unit abbreviations](#units)  if they match the unit for the metric. It is sometimes the case that the metric name indicates the unit, in which case the unit does not need to be specified separately. For example, something that counts bits can be expressed as `bit_count` where `bit` is considered the name and `count` is the aggregation function . In such a case though, it is recommended not to use an abbreviation. For example, `packet_count` and not `pkt_count`.  
* If the metric represents a stat that was aggregated then include the `<aggregation_function>` and `<aggregation_window>`. You do not need to include either if they are indicated by the table name. **Example:** `cpu_load_avg_1m`, `cpu_load_avg_5m`

## Adding Additional Fields with ext

A special field named `ext` is used for extending the table with additional attributes and metrics without requiring the basic table structure to be altered. It is a [JSON column](https://clickhouse.com/docs/sql-reference/data-types/newjson) that allows subcolumns to be created dynamically. The following considerations should be taken for this field:

* All fields in `ext` must follow regular field naming conventions.   
* Fields can be attributes, metrics or references to other tables  
* The structure should be flat and nesting should generally be avoided  
* Subcolumns are accessed using dot notation **Examples:** `ext.in_interface_id`, `ext.in_interface_ref, ext.vendor_attribute, ext.custom_metric`  
* Up to a certain number of fields controlled by a ClickHouse setting these are proper subcolumns and can be used in primary keys and indices. After this point they become less efficient Map structures. As such, it is generally better to define fields as proper columns if they are generally applicable and well known. This reduces the chances of hitting this limit. The `ext` field is ideal for columns that may not be applicable in every deployment or where deployment-specific fields are needed but it is not desirable to alter the table structure or create a separate table.

# Skip Index Naming Conventions

The primary type of index in clickhouse is a skip index. A skip index is dissimilar to a standard database index. Rather than providing offsets of particular rows that match a filter value as in a standard index, a skip index indicates only whether a filter match is present in a particular “part.”  
For more on skip indexes: [https\://clickhouse.com/docs/optimize/skipping-indexes](https://clickhouse.com/docs/optimize/skipping-indexes)

A skip index applies to a table but requires a name similar to a field. A skip index uses the following naming format:

`<field_name>_<index_type>_index`

* `<field_name>` is the name of the field being indexed, If it is a subcolumn from the ext field, replace the dot(.) with an underscore(\_).  
* `<index_type>` should be the time of index such as minmax, set or bloom

**Examples:**

* `src_asn_set_index`  
* `Ext_bgp_asn_path_bloom_index`

# Projection Naming Conventions

Projections are another clickhouse-specific concept. They are hidden tables with a re-organized presentation of a visible table, usually with a re-optimized primary or sort key. They are used implicitly by the query planner to more quickly answer queries that follow particular access/query patterns. For more on projections: [https\://clickhouse.com/docs/data-modeling/projections](https://clickhouse.com/docs/data-modeling/projections)

Projections should be named for their purpose and have a suffix of `proj`. A projection must have a `distinguisher` that describes the differences between the main table and the projection. The naming convention in summarized by the format below:

`<distinguisher>_proj`

**Examples:**

* `by_src_proj`  
* `delta_proj`

# Common Abbreviations {#common-abbreviations}

This section contains common abbreviations used in table names and field names. 

### Aggregation Functions {#aggregation-functions}

* `avg` \- The average or mean  
* `count` \- The number of occurrences  
* `iqr` \- The interquartile range  
* `latest` \- The most recent instance of target attribute(s) or metric(s)  
* `histogram` \- Values aggregated into a histogram  
* `max` \- The maximum value  
* `min` \- The minimum value  
* `p<percentile>` \- The `<percentile>`th percentile. E.g. p95 means 95th percentile. Note the median is p50.   
* `stdev` \- The standard deviation  
* `total` \- The sum of all the values

### Directionality

* `in` \- The incoming, receiving and/or ingress direction  
* `out` \- The outgoing, sending and/or egress direction  
* `src` \- The source or origin point of a communication between parties  
* `dst` \- The destination or target point of a communication between parties

### Units {#units}

* `bits` \- Bits  
* `bps` \- Bits per second  
* `bytes` \- Bytes  
* `pkts` \- Packets  
* `pps` \- Packets per second 