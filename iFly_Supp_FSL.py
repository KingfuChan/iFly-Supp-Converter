import os
import shutil
import math
from zipfile import ZipFile, ZIP_DEFLATED
import pandas as pd

fsl_base_dir = "FSL-2313"
output_dir = "iFly-2313"

df_airport = pd.read_csv(f"{fsl_base_dir}/AIRPORT.csv")
df_runway = pd.read_csv(f"{fsl_base_dir}/RUNWAY.csv")
df_proc = pd.read_csv(f"{fsl_base_dir}/AIRPORT_PROCEDURE.csv")
df_waypoint = pd.read_csv(f"{fsl_base_dir}/WAYPOINT.csv")
df_vhf = pd.read_csv(f"{fsl_base_dir}/VHF_NAVAID.csv")
df_ndb = pd.read_csv(f"{fsl_base_dir}/NDB_NAVAID.csv")


def main() -> None:
    try:
        # restore directories
        shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir)
        os.makedirs(os.path.join(output_dir, "Supp"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "Star"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "Sid"), exist_ok=True)
        # process
        export_airport_supp()
        export_airport_sid()
        export_airport_star()
        export_airport_app()
        # pack
        shutil.copy("Installation.txt", f"{output_dir}/Installation.txt")
        with ZipFile(f"{output_dir}-CHN-PROC-FULL.zip", 'w',
                     compression=ZIP_DEFLATED, compresslevel=9) as zipf:
            for root, dirs, files in os.walk(output_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, output_dir)
                    zipf.write(file_path, arcname=arcname)
    finally:
        open("debug.txt", 'w', newline='\r\n').write('\n'.join(debug_lines))


def export_airport_supp() -> None:
    df_airport.loc[:, 'TRANSITIONS_ALT'].fillna(9800, inplace=True)
    df_airport.loc[:, 'TRANSITION_LEVEL'].fillna(11800, inplace=True)
    for _, row in df_airport.iterrows():
        arpt_name = row['ARPT_IDENT']
        lines = ["[Speed_Transition]"]
        lines.append("Speed=250")
        arpt_alt = int(row['ARPT_ELEV'])
        arpt_alt = 0 if arpt_alt < 5000 else arpt_alt
        spd_alt = int((arpt_alt+10000)/1000)*1000
        lines.append(f"Altitude={spd_alt}")
        lines.append("[Transition_Altitude]")
        trs_alt = int(row['TRANSITIONS_ALT'])
        lines.append(f"Altitude={trs_alt}")
        lines.append("[Transition_Level]")
        trs_lvl = int(row['TRANSITION_LEVEL'])
        lines.append(f"Altitude={trs_lvl}")
        open(f"{output_dir}/Supp/{arpt_name}.supp", 'w',
             newline='\r\n').write('\n'.join(lines))
        print(f"Exported: {arpt_name}.supp")


def export_airport_sid() -> None:
    arpt_list = df_proc.loc[(df_proc['SUBS_CODE'] == 'D') &
                            (df_proc['ROUTE_TYPE'].isin(
                                ['0', '1', '2', '3', '4', '5', '6'])),
                            'ARPT_IDENT'].value_counts(sort=False).index.to_list()
    for arpt in arpt_list:  # by airport
        df_arpt_proc = df_proc[(df_proc['ARPT_IDENT'] == arpt) &
                               (df_proc['SUBS_CODE'] == 'D') &  # departure
                               (df_proc['ROUTE_TYPE'].isin(['1', '2', '3', '4', '5', '6']))]  # RNAV SID
        # structure: {type:{procedure:[[leg,],]}}
        dict_arpt = {'main': {}, 'trans': {}}
        for proc_type in ['0', '1', '2', '3', '4', '5', '6']:  # force sequence
            # by procedure
            df_arpt_proc_type = df_arpt_proc[df_arpt_proc['ROUTE_TYPE'] == proc_type]
            if not df_arpt_proc_type.shape[0]:
                continue
            for dict_proc in split_procedure(df_arpt_proc_type, 'SEQ_NR'):
                proc_name = dict_proc['ident']
                proc_conn = dict_proc['conn']
                proc_legs = [extract_leg(r)
                             for _, r in dict_proc['legs'].iterrows()]
                if proc_type in ['3', '6']:  # SID trans
                    dict_arpt['trans'][f"{proc_conn}.{proc_name}"] = proc_legs
                    continue
                arpt_rwys = df_runway.loc[df_runway['ARPT_IDENT'] == arpt,
                                          'RUNWAY_IDENT'].to_list()
                if pd.isna(proc_conn) or proc_conn == 'ALL':
                    is_extended = False
                    # find a previous procedure with same ident
                    for pn in dict_arpt['main'].keys():
                        if pn.split('.')[0] == proc_name:
                            # drop first point (IF leg)
                            dict_arpt['main'][pn].extend(proc_legs[1:])
                            is_extended = True
                    if is_extended:
                        continue
                    else:
                        proc_conn = 'RW'  # eliminate na
                if proc_conn[-1] == 'B':
                    proc_conn = proc_conn[:-1]
                for rw in arpt_rwys:  # rw is like "RW09L"
                    if proc_conn in rw:
                        dict_arpt['main'][f"{proc_name}.{rw[2:]}"] = proc_legs
        # organize this airport
        for pt in ['main', 'trans']:
            if len(dict_arpt[pt]) == 0:
                continue
            full_lines = ["[list]"]
            i = 0
            for pn in sorted(dict_arpt[pt].keys()):
                full_lines.append(f"Procedure.{i}={pn}")
                i += 1
            for pn in sorted(dict_arpt[pt].keys()):  # pn:str, pl:list
                k = 0
                pl = dict_arpt[pt][pn]
                for ls in pl:  # ls:list
                    full_lines.append(f"[{pn}.{k}]")
                    full_lines.extend(ls)
                    k += 1
            filename = f"{arpt}.sid{'trs' if pt=='trans' else ''}"
            open(f"{output_dir}/Sid/{filename}", 'w',
                 newline='\r\n').write('\n'.join(full_lines))
            print(f"Exported: {filename}")


def export_airport_star() -> None:
    arpt_list = df_proc.loc[(df_proc['SUBS_CODE'] == 'E') &
                            (df_proc['ROUTE_TYPE'].isin(
                                ['3', '2', '1', '6', '5', '4'])),
                            'ARPT_IDENT'].value_counts(sort=False).index.to_list()
    for arpt in arpt_list:  # by airport
        df_arpt_proc = df_proc[(df_proc['ARPT_IDENT'] == arpt) &
                               (df_proc['SUBS_CODE'] == 'E') &  # arrival
                               (df_proc['ROUTE_TYPE'].isin(
                                   ['3', '2', '1', '6', '5', '4']))]  # RNAV STAR
        # structure: {type:{procedure:[[leg,],]}}
        dict_arpt = {'main': {}, 'trans': {}}
        for proc_type in ['3', '2', '1', '6', '5', '4']:  # force sequence
            # by procedure
            df_arpt_proc_type = df_arpt_proc[df_arpt_proc['ROUTE_TYPE'] == proc_type]
            if not df_arpt_proc_type.shape[0]:
                continue
            for dict_proc in split_procedure(df_arpt_proc_type, 'SEQ_NR'):
                proc_name = dict_proc['ident']
                proc_conn = dict_proc['conn']
                proc_legs = [extract_leg(r)
                             for _, r in dict_proc['legs'].iterrows()]
                if proc_type in ['1', '4']:  # STAR trans
                    dict_arpt['trans'][f"{proc_conn}.{proc_name}"] = proc_legs
                    continue
                arpt_rwys = df_runway.loc[df_runway['ARPT_IDENT'] == arpt,
                                          'RUNWAY_IDENT'].to_list()
                if pd.isna(proc_conn) or proc_conn == 'ALL':
                    is_extended = False
                    # find a previous procedure with same ident
                    for pn in dict_arpt['main'].keys():
                        if pn.split('.')[0] == proc_name:
                            # drop first point (IF leg)
                            dict_arpt['main'][pn].extend(proc_legs[1:])
                            is_extended = True
                    if is_extended:
                        continue
                    else:
                        proc_conn = 'RW'  # eliminate na
                if proc_conn[-1] == 'B':
                    proc_conn = proc_conn[:-1]
                for rw in arpt_rwys:  # rw is like "RW09L"
                    if proc_conn in rw:
                        dict_arpt['main'][f"{proc_name}.{rw[2:]}"] = proc_legs
        # organize this airport
        for pt in ['main', 'trans']:
            if len(dict_arpt[pt]) == 0:
                continue
            full_lines = ["[list]"]
            i = 0
            for pn in sorted(dict_arpt[pt].keys()):
                full_lines.append(f"Procedure.{i}={pn}")
                i += 1
            for pn in sorted(dict_arpt[pt].keys()):  # pn:str, pl:list
                k = 0
                pl = dict_arpt[pt][pn]
                for ls in pl:  # ls:list
                    full_lines.append(f"[{pn}.{k}]")
                    full_lines.extend(ls)
                    k += 1
            filename = f"{arpt}.star{'trs' if pt=='trans' else ''}"
            open(f"{output_dir}/Star/{filename}", 'w',
                 newline='\r\n').write('\n'.join(full_lines))
            print(f"Exported: {filename}")


def export_airport_app() -> None:
    arpt_list = df_proc.loc[df_proc['SUBS_CODE'] == 'F',
                            'ARPT_IDENT'].value_counts(sort=False).index.to_list()
    for arpt in arpt_list:  # by airport
        df_arpt_proc = df_proc[(df_proc['ARPT_IDENT'] == arpt) &
                               (df_proc['SUBS_CODE'] == 'F')]  # approach
        # structure: {type:{procedure:[[leg,],]}}
        dict_arpt = {'main': {}, 'trans': {}}
        # by procedure
        for dict_proc in split_procedure(df_arpt_proc, 'SEQ_NR'):
            proc_name = dict_proc['ident']
            proc_conn = dict_proc['conn']
            proc_type = dict_proc['type']
            proc_legs = [extract_leg(r)
                         for _, r in dict_proc['legs'].iterrows()]
            if proc_type == 'A':  # approach trans
                dict_arpt['trans'][f"{proc_conn}.{proc_name}"] = proc_legs
            else:  # approach, need to parse runway ident from procedure ident
                rw_ident = str(proc_name[1:3])
                if rw_ident.isdigit():
                    if len(proc_name) > 3:  # contains L/R and/or W/X/Y/Z
                        rw_ident += proc_name[3] if proc_name[3] in ['L', 'R'] else ""
                    dict_arpt['main'][f"{proc_name}.{rw_ident}"] = proc_legs
                else:  # in case not specified, e.g. ZYJM:CNDB
                    arpt_rwys = df_runway.loc[df_runway['ARPT_IDENT'] == arpt,
                                              'RUNWAY_IDENT'].to_list()
                    for rw in arpt_rwys:  # rw is like "RW09L"
                        dict_arpt['main'][f"{proc_name}.{rw[2:]}"] = proc_legs
                        print_debug_message(
                            f"Warning: uncertain runway, added to all. {arpt}:{proc_name}:{proc_conn}")
        # organize this airport
        for pt in ['main', 'trans']:
            if len(dict_arpt[pt]) == 0:
                continue
            full_lines = ["[list]"]
            i = 0
            for pn in sorted(dict_arpt[pt].keys()):
                full_lines.append(f"Procedure.{i}={pn}")
                i += 1
            for pn in sorted(dict_arpt[pt].keys()):  # pn:str, pl:list
                k = 0
                pl = dict_arpt[pt][pn]
                for ls in pl:  # ls:list
                    full_lines.append(f"[{pn}.{k}]")
                    full_lines.extend(ls)
                    k += 1
            filename = f"{arpt}.app{'trs' if pt=='trans' else ''}"
            open(f"{output_dir}/Star/{filename}", 'w',
                 newline='\r\n').write('\n'.join(full_lines))
            print(f"Exported: {filename}")


def split_procedure(df: pd.DataFrame, col: str) -> dict:
    """return a dict of split DataFrame, structure: [{'ident':str, 'type':str , 'conn':str, 'legs': DataFrame}]"""
    df.reset_index(drop=True, inplace=True)
    split_indices = df[df[col].shift(1) >= df[col]].index
    split_dataframes = []
    start_index = 0
    for end_index in split_indices:
        if end_index < 1:
            continue
        group_df = df.loc[start_index:end_index-1]
        split_dataframes.append(group_df)
        start_index = end_index
    last_group_df = df.loc[start_index:]
    split_dataframes.append(last_group_df)
    res = []
    for sd in split_dataframes:
        sd.reset_index(drop=True, inplace=True)
        res.append({
            'ident': sd['PROC_IDENT'].iloc[0],
            'type': sd['ROUTE_TYPE'].iloc[0],
            'conn': sd['TRANSITION_IDENT'].iloc[0],
            'legs': sd,
        })
    return res


def extract_leg(row: pd.Series) -> list:
    """return a list of lines (without header)"""
    arpt = row['ARPT_IDENT']
    leg_type = row['PATH_AND_TERMINATION']
    proc_name = row['PROC_IDENT']
    extracted_lines = [f"Leg={leg_type}"]
    pt_name = row['FIX_IDENT']
    # find Lat/Lon
    if leg_type in ['PI', 'HA', 'HF', 'HM', 'AF', 'CF', 'DF', 'FC', 'FD', 'RF', 'TF', 'IF']:
        if not pd.isna(pt_name):
            extracted_lines.append(f"Name={pt_name}")
            latitude, longitude, msg = (0, 0, "")
            if row['FIX_SUBS_CODE'] == 'G':  # use runway csv
                rw_row = df_runway[(df_runway['ARPT_IDENT'] == arpt) &
                                   (df_runway['RUNWAY_IDENT'] == pt_name)]
                if rw_row.shape[0]:
                    latitude = rw_row['RUNWAY_LAT'].iloc[0]
                    longitude = rw_row['RUNWAY_LON'].iloc[0]
            else:
                latitude, longitude, msg = find_a_point(
                    pt_name, arpt, row['FIX_SECT_CODE'], row['FIX_SUBS_CODE'])
            if latitude and longitude:
                extracted_lines.append("Latitude=%.06f" % latitude)
                extracted_lines.append("Longitude=%.06f" % longitude)
            if len(msg):
                print_debug_message(
                    f"Warning: Lat/Lon for {arpt}:{proc_name}:{pt_name}:{msg}")
        else:
            print_debug_message(
                f"Warning: IDENT missing for {arpt}:{proc_name}")
    # cross this point: by finding 'B/Y' in 2nd char of WAYPOINT_DESCR_CODE
    pt_descr = row['WAYPOINT_DESCR_CODE']
    if not pd.isna(pt_descr) and len(pt_descr) == 4 and pt_descr[1] in ['B', 'Y']:
        extracted_lines.append("CrossThisPoint=1")
    # heading
    pt_hdg = row['MAG_COURSE']
    if leg_type in ['PI', 'HA', 'HF', 'HM', 'FM', 'VM', 'CA', 'VA', 'CD', 'VD', 'CF', 'CI', 'VI', 'CR', 'VR', 'FA', 'FC', 'FD']:
        if not pd.isna(pt_hdg):
            extracted_lines.append("Heading=%.01f" % float(pt_hdg))
        else:
            print_debug_message(
                f"Warning: Heading missing for {arpt}:{proc_name}:{pt_name}")
    # turn direction
    pt_tdir = row['TURN_DIR']
    if pt_tdir in ['L', 'R']:
        extracted_lines.append(f"TurnDirection={pt_tdir}")
    elif leg_type in ['PI', 'HA', 'HF', 'HM']:
        print_debug_message(
            f"Warning: TurnDirection missing for {arpt}:{proc_name}:{pt_name}")
    # speed
    pt_spd = row['SPEED_LIMIT']
    if not pd.isna(pt_spd) and len(pt_spd := pt_spd.strip()):
        pt_spd_descr = row['SPEED_LIMIT_DESCR']
        if pt_spd_descr == '+':
            extracted_lines.append(f"Speed={pt_spd}A")
        elif pt_spd_descr == '-':
            extracted_lines.append(f"Speed={pt_spd}B")
        else:
            extracted_lines.append(f"Speed={pt_spd}")
    # altitude
    pt_alt1 = row['ALT_1']
    if not pd.isna(pt_alt1):
        pt_alt_descr = row['ALT_DESCR']
        # discrepancies with ARINC-424: G/H/I-@; J-+
        if pt_alt_descr in ['+', 'C', 'J', 'V']:
            extracted_lines.append("Altitude=%dA" % pt_alt1)
        elif pt_alt_descr in ['-', 'Y']:
            extracted_lines.append("Altitude=%dB" % pt_alt1)
        elif pt_alt_descr == 'B':
            pt_alt2 = row['ALT_2']
            extracted_lines.append("Altitude=%dA%dB" % (pt_alt2, pt_alt1))
        else:
            extracted_lines.append("Altitude=%d" % pt_alt1)
    elif leg_type in ['CA', 'VA', 'FA']:
        print_debug_message(
            f"Warning: Altitude missing for {arpt}:{proc_name}:{pt_name}")
    # missed approach point: by finding 'M' in 4th char of WAYPOINT_DESCR_CODE
    if not pd.isna(pt_descr) and len(pt_descr) == 4 and pt_descr[3] == 'M':
        extracted_lines.append("MAP=1")
    # frequency (using ident)
    pt_navaid = row['RECOMMENDED_NAVAID']
    if not pd.isna(pt_navaid) and len(pt_navaid := pt_navaid.strip()):
        extracted_lines.append(f"Frequency={pt_navaid}")
    elif leg_type in ['PI', 'AF', 'CD', 'VD', 'CR', 'VR', 'FD']:
        print_debug_message(
            f"Warning: Frequency missing for {arpt}:{proc_name}:{pt_name}")
    # slope
    pt_angl = row['VERTICAL_ANGLE']
    if not pd.isna(pt_angl):
        extracted_lines.append(f"Slope={-float(pt_angl)}")
    # NavBear
    pt_navbear = row['THETA']
    if not pd.isna(pt_navbear):
        extracted_lines.append("NavBear=%.01f" % (int(pt_navbear)/10))
    elif leg_type in ['PI', 'CR', 'VR']:
        print_debug_message(
            f"Warning: NavBear missing for {arpt}:{proc_name}:{pt_name}")
    # NavDist
    if leg_type in ['CD', 'VD', 'FD']:
        pt_dort = row['ROUTE_DISTANCE_HOLDING_DISTANCE_OR_TIME']
        if not pd.isna(pt_dort) and len(pt_dort := pt_dort.strip()) and pt_dort.isdigit():
            extracted_lines.append("NavDist=%.01f" % (int(pt_dort)/10))
        else:
            print_debug_message(
                f"Warning: NavDist missing for {arpt}:{proc_name}:{pt_name}")
    else:
        pt_navrho = row['RHO']
        if not pd.isna(pt_navrho):
            extracted_lines.append("NavDist=%.01f" % (int(pt_navrho)/10))
        elif leg_type in ['PI', 'AF']:
            print_debug_message(
                f"Warning: NavDist missing for {arpt}:{proc_name}:{pt_name}")
    # dist
    pt_dort = row['ROUTE_DISTANCE_HOLDING_DISTANCE_OR_TIME']
    if not pd.isna(pt_dort) and len(pt_dort := pt_dort.strip()) and pt_dort[0] == 'T':
        extracted_lines.append("Dist=%d" % (int(pt_dort[1:])*1000))  # time
    elif not pd.isna(pt_dort) and pt_dort.isdigit():  # n miles
        extracted_lines.append("Dist=%.01f" % (int(pt_dort)/10))
    elif leg_type in ['PI', 'HA', 'HF', 'HM', 'FC']:
        print_debug_message(
            f"Warning: Dist missing for {arpt}:{proc_name}:{pt_name}")
    # Center lat/lon
    if leg_type == 'RF':
        pt_cfix = row['CENTER_FIX_OR_TAA_PROCEDURE_TURN_IND'].strip()
        if len(pt_cfix):
            latitude, longitude, msg = find_a_point(
                pt_cfix, arpt, row['MULTIPLE_CODE_OR_TAA_SECTOR_SECT_CODE'], row['MULTIPLE_CODE_OR_TAA_SECTOR_SUBS_CODE'])
            if latitude and longitude:
                extracted_lines.append("CenterLat=%.06f" % latitude)
                extracted_lines.append("CenterLon=%.06f" % longitude)
            if len(msg):
                print_debug_message(
                    f"Warning: RF center for {arpt}:{proc_name}:{pt_cfix}:{msg}")
        else:
            print_debug_message(
                f"Warning: RF center missing for {arpt}:{proc_name}:{pt_name}")
    return extracted_lines


def find_a_point(ident: str, airport: str, sect_code: str, subs_code: str) -> tuple:
    """returns (lat, lon, msg) tuple"""
    arpt_row = df_airport[df_airport['ARPT_IDENT'] == airport]
    if not arpt_row.shape[0]:
        return (0, 0, "airport not found")
    arpt_lat, arpt_lon = arpt_row.iloc[0][['ARPT_LAT', 'ARPT_LON']].to_list()
    if sect_code == 'E':  # enroute waypoint
        p_match = df_waypoint[(df_waypoint['WAYPOINT_IDENT'] == ident) &
                              (df_waypoint['SECT_CODE'] == 'E')]
        if p_match.shape[0]:
            p_all = pd.DataFrame(
                {'LAT': p_match['WAYPOINT_LAT'],
                 'LON': p_match['WAYPOINT_LON'],
                 'DIS': 1000})
        else:
            return (0, 0, "enroute waypoint not found")
    elif sect_code == 'P' and subs_code == 'C':  # terminal waypoint
        p_match = df_waypoint[(df_waypoint['WAYPOINT_IDENT'] == ident) &
                              (df_waypoint['REGION_CODE'] == airport)]
        if p_match.shape[0]:
            p_all = pd.DataFrame(
                {'LAT': p_match['WAYPOINT_LAT'],
                 'LON': p_match['WAYPOINT_LON'],
                 'DIS': 1000})
        else:
            return (0, 0, "terminal waypoint not found")
    elif sect_code == 'D' and (pd.isna(subs_code) or not len(subs_code.strip())):  # VOR
        p_match = df_vhf[df_vhf['VOR_IDENT'] == ident]
        if p_match.shape[0]:
            p_all = pd.DataFrame(
                {'LAT': p_match['VOR_LAT'],
                 'LON': p_match['VOR_LON'],
                 'DIS': 1000})
        else:
            return (0, 0, "VOR not found")
    elif (sect_code == 'D' and subs_code == 'B') or \
            (sect_code == 'P' and subs_code == 'N'):  # NDB
        p_match = df_ndb[(df_ndb['NDB_IDENT'] == ident) &
                         (df_ndb['SECT_CODE'] == sect_code)]
        if p_match.shape[0]:
            p_all = pd.DataFrame(
                {'LAT': p_match['NDB_LAT'],
                 'LON': p_match['NDB_LON'],
                 'DIS': 1000})
        else:
            return (0, 0, "NDB not found")
    elif sect_code == 'P' and subs_code == 'G':  # runway
        p_match = df_runway[(df_runway['ARPT_IDENT'] == airport) &
                            (df_runway['RUNWAY_IDENT'] == ident)]
        if p_match.shape[0]:
            p_all = pd.DataFrame(
                {'LAT': p_match['RUNWAY_LAT'],
                 'LON': p_match['RUNWAY_LON'],
                 'DIS': 1000})
        else:
            return (0, 0, "runway not found")
    else:  # unknown
        return (0, 0, "unknown type point")
    p_all['DIS'] = p_all[['LAT', 'LON']].apply(lambda r: calculate_distance(
        r['LAT'], r['LON'], arpt_lat, arpt_lon), axis=1)
    p_all.sort_values('DIS', ascending=True, inplace=True)
    msg = "too far" if p_all.iloc[0, 2] >= 1000 else ""
    return (p_all.iloc[0, 0], p_all.iloc[0, 1], msg)


def calculate_distance(lat1, lon1, lat2, lon2):
    """Haversine formula. Args in degrees. Returns in kilometers."""
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    earth_radius = 6371
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * \
        math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    distance = earth_radius * c
    return distance


def print_debug_message(msg: str) -> None:
    debug_lines.append(msg)
    print(msg)


if __name__ == "__main__":
    global debug_lines
    debug_lines = []
    main()
