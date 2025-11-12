class OIDFormatUtil:
    """
    Utility class for formatting SNMP OID indices into readable formats.
    """
    
    @classmethod
    def str_format_ip_mib_entry_index(cls, v):
        """
        Format a string in format ADDR_TYPE.NUM_OCTETS.DOTTED_IP
        
        IPv4 Example: 1.4.198.128.59.2 (1 means IPv4, 4 octets, IP 198.128.59.2)
        IPv6 Example: 2.16.32.1.4.0.230.0.80.32.0.0.0.0.0.0.0.0 (2 means IPv6, 16 octets, rest needs formatting)
        
        Args:
            v: String in format "ADDR_TYPE.NUM_OCTETS.DOTTED_IP"
            
        Returns:
            Formatted IP address string, or None if invalid
        """
        # Make sure we have a value
        if not v:
            return None
            
        try:
            # Strip leading and trailing whitespace
            v = str(v).strip()
            
            # Split by dots
            v_parts = v.split(".")
            
            # Must have at least 6 parts to be valid
            if len(v_parts) < 6:
                return None
                
            # Get address type
            addr_type = int(v_parts.pop(0))
            
            # Don't need octets for anything so discard - v4 is always 4 and v6 is always 16
            v_parts.pop(0)
            
            # Figure out what to do based on the address type
            if addr_type == 1:
                # If it's IPv4 then it's easy, just list separated by dots (addrtype 1 is IPv4)
                if len(v_parts) != 4:
                    return None
                v = '.'.join(v_parts)
            elif addr_type == 2:
                # If it's IPv6 then get out of dotted notation and make look like IPv6 (addrtype 2 is IPv6)
                # Use long form notation since easiest
                if len(v_parts) != 16:
                    return None
                    
                ipv6_str = ""
                for i in range(16):
                    # Add separator every other iteration
                    if i != 0 and (i % 2) == 0:
                        ipv6_str += ":"
                    # Convert to hex string with leading 0s
                    ipv6_str += format(int(v_parts[i]), '02x')
                v = ipv6_str
            else:
                # There are other address types for link local, etc but we don't care about those
                return None
                
            return v
            
        except (ValueError, IndexError, AttributeError):
            return None

class TmnxPortId:
    """
    Nokia/Timetra Port ID decoder for SNMP OIDs.
    
    Note that we use class variables and methods to keep things scoped
    but to limit the need to instantiate an object every time the
    filter is called.

    TmnxPortID decoding based on information in TIMETRA-TC-MIB.
    type_code = (x & int('11100000000000000000000000000000', 2)) >> 29

    A portid is an unique 32 bit number encoded as shown below.

        32 30 | 29 26 | 25 22 | 21 16 | 15  1 |
        +-----+-------+-------+-------+-------+
        |000  |  slot |  mda  | port  |  zero | Physical Port
        +-----+-------+-------+-------+-------+

        32 30 | 29 26 | 25 22 | 21 16 | 15  1 |
        +-----+-------+-------+-------+-------+
        |001  |  slot |  mda  | port  |channel| Channel
        +-----+-------+-------+-------+-------+

    Slots, mdas (if present), ports, and channels are numbered
    starting with 1.

        32     29 | 28             10 | 9   1 |
        +---------+-------------------+-------+
        | 0 1 0 0 |   zeros           |   ID  | Virtual Port
        +---------+-------------------+-------+

        32     29 | 28                9 | 8 1 |
        +---------+---------------------+-----+
        | 0 1 0 1 |   zeros             | ID  | LAG Port
        +---------+---------------------+-----+

    A card port number (cpn) has significance within the context
    of the card on which it resides(ie., cpn 2 may exist in one or
    more cards in the chassis).  Whereas, portid is an
    unique/absolute port number (apn) within a given chassis.

    An 'invalid portid' is a TmnxPortID with a value of 0x1e000000 as
    represented below.

        32 30 | 29 26 | 25 22 | 21 16 | 15  1 |
        +-----+-------+-------+-------+-------+
        |zero | ones  | zero  |  zero |  zero | Invalid Port
        +-----+-------+-------+-------+-------+

    The oidIndex for items in TIMETRA-SAP-MIB::sapBaseInfoTable are structured
    as follows:  svcIf.sapPortId.sapEncapValue

    sapBaseInfoEntry                 OBJECT-TYPE
    SYNTAX      SapBaseInfoEntry
    MAX-ACCESS  not-accessible
    STATUS      current
    DESCRIPTION "Information about a specific SAP."
    INDEX       {svcId, sapPortId, sapEncapValue }

    Note that the Nokias introduced something called a BreakoutConnection
    port which deviates from the latest TIMETRA-TC-MIB and TIMETRA-PORT-MIB.
    The ID follows a similar structure to "Scheme B" from the related
    TIMETRA-CHASSIS-MIB but deviates in the last 14 bits. The port names take
    the form of SLOT/MDA/cX/Y (where c is the character 'c' and X and Y are
    derived from last 14 bits). On comparing the bit structure to known ports
    of this type, the bit structure was determined to be as follows:

    |32 30| 29 24 | 23 19 | 18 15 | 14  13 | 12 7 | 6  1 |
    +-----+-------+-------+-------+--------+------+------+
    |011  |  zero |  slot |  mda  |  10    | X    | Y    |
    +-----+-------+-------+-------+--------+------+------+
    """
    
    # Class constants
    INVALID = 0x1e000000
    TYPE_MASK = 0xe0000000
    TYPE_SHIFT = 29
    SLOT_MASK = 0x1e000000
    SLOT_SHIFT = 25
    MDA_MASK = 0x1e00000
    MDA_SHIFT = 21
    PORT_MASK = 0x1f8000
    PORT_SHIFT = 15
    CHANNEL_MASK = 0x7fff
    VIRTUALPORT_MASK = 0x1ff
    LAG_MASK = 0xff
    CHANNEL_VIRTUALPORT_MASK = 0x10000000
    CHANNEL_VIRTUALPORT_SHIFT = 28
    VIRTUAL_PREFIX = 0b0100 << CHANNEL_VIRTUALPORT_SHIFT
    LAG_PREFIX = 0b0101 << CHANNEL_VIRTUALPORT_SHIFT

    PHYSICAL_PORT_TYPE = 0b000
    CHANNEL_PORT_TYPE = 0b001
    VIRT_PORT_OR_LAG_TYPE = 0b010
    BREAKOUT_CONN_TYPE = 0b011

    @classmethod
    def decode(cls, port_id):
        """
        Decode a Nokia/Timetra port ID into a human-readable port name.
        
        Args:
            port_id: Port ID as integer or string
            
        Returns:
            String representation of the port name, or None if invalid
        """
        # Make sure we have an int
        try:
            port_id = int(port_id)
        except (ValueError, TypeError):
            return None
            
        port_type = (port_id & cls.TYPE_MASK) >> cls.TYPE_SHIFT
        slot = (port_id & cls.SLOT_MASK) >> cls.SLOT_SHIFT
        mda = (port_id & cls.MDA_MASK) >> cls.MDA_SHIFT
        port = (port_id & cls.PORT_MASK) >> cls.PORT_SHIFT

        # Check for the special invalid port id as defined by spec
        if port_id == cls.INVALID:
            return None

        # Based on type, parse the id or return None if unrecognized
        if port_type == cls.BREAKOUT_CONN_TYPE:
            brslot = (port_id & 0x7c0000) >> 18
            brmda = (port_id & 0x3c000) >> 14
            brc = (port_id & 0xfc0) >> 6
            brlast = (port_id & 0x3f)
            return f"{brslot}/{brmda}/c{brc}/{brlast}"
        elif port_type == cls.PHYSICAL_PORT_TYPE:
            return f"{slot}/{mda}/{port}"
        elif port_type == cls.CHANNEL_PORT_TYPE:
            channel = port_id & cls.CHANNEL_MASK
            return f"{slot}/{mda}/{port}.{channel}"
        elif port_type == cls.VIRT_PORT_OR_LAG_TYPE:
            channel_virtual_port = (port_id & cls.CHANNEL_VIRTUALPORT_MASK) >> cls.CHANNEL_VIRTUALPORT_SHIFT
            if channel_virtual_port == 0:
                virtual_port = port_id & cls.VIRTUALPORT_MASK
                return f"virtual-{virtual_port}"
            elif channel_virtual_port == 1:
                lag = port_id & cls.LAG_MASK
                return f"lag-{lag}"

        return None

    @classmethod
    def decode_sap(cls, sap_id, ext=None):
        """
        Parse SAP ID in format prefix.portid.suffix and convert to 
        prefix-name-suffix format.
        
        Args:
            sap_id: SAP ID string in format "prefix.portid.suffix"
            
        Returns:
            String in format "prefix-portname-suffix", or None if invalid
        """
        # Verify it is a valid id
        if sap_id is None:
            return None
            
        try:
            id_parts = str(sap_id).split('.')
            if len(id_parts) < 3 or len(id_parts) > 4:
                return None

            # Get portname
            port_name = cls.decode(id_parts[1])
            if port_name is None:
                return None

            #extract queue_num if present
            if ext is not None and len(id_parts) == 4:
                ext['queue_num'] = id_parts[3]

            # Return formatted id
            return f"{id_parts[0]}-{port_name}-{id_parts[2]}"
        except (AttributeError, IndexError):
            return None

