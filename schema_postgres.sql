--
-- PostgreSQL database dump
--

-- Dumped from database version 16.9 (Debian 16.9-1.pgdg120+1)
-- Dumped by pg_dump version 16.9 (Debian 16.9-1.pgdg120+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: match; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match (
    match_id text NOT NULL,
    match_date date,
    home_team_id text,
    away_team_id text,
    season_id bigint,
    match_type_id text,
    match_type_name text,
    xg_home real,
    xg_away real,
    wk bigint,
    match_subtype_id text,
    match_subtype_name text,
    home_team_name text,
    away_team_name text,
    home_goals bigint,
    away_goals bigint,
    home_team_season_id text,
    away_team_season_id text
);


ALTER TABLE public.match OWNER TO postgres;

--
-- Name: match_goalkeeper_summary; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_goalkeeper_summary (
    match_goalkeeper_id text NOT NULL,
    match_id text,
    player_id text,
    player_name text,
    team_id text,
    team_name text,
    season_id bigint,
    nation text,
    age text,
    minutes_played bigint,
    shots_on_target_against bigint,
    goals_against bigint,
    saves bigint,
    save_percentage real,
    post_shot_xg real,
    launched_cmp bigint,
    launched_att bigint,
    launched_cmp_pct real,
    passes_att bigint,
    passes_thr bigint,
    passes_launch_pct real,
    passes_avg_len real,
    goal_kicks_att bigint,
    goal_kicks_launch_pct real,
    goal_kicks_avg_len real,
    crosses_opp bigint,
    crosses_stp bigint,
    crosses_stp_pct real,
    sweeper_opa bigint,
    sweeper_avg_dist real,
    match_player_id text
);


ALTER TABLE public.match_goalkeeper_summary OWNER TO postgres;

--
-- Name: match_lineup; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_lineup (
    lineup_id text NOT NULL,
    match_id text,
    team_id text,
    player_id text,
    player_name text,
    "position" text,
    jersey_number bigint,
    is_starter boolean DEFAULT true,
    formation text
);


ALTER TABLE public.match_lineup OWNER TO postgres;

--
-- Name: match_player; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_player (
    match_player_id text NOT NULL,
    match_id text,
    player_id text,
    player_name text,
    team_id text,
    shirt_number bigint,
    minutes_played bigint,
    season_id text
);


ALTER TABLE public.match_player OWNER TO postgres;

--
-- Name: match_player_defensive_actions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_player_defensive_actions (
    match_player_defensive_actions_id text NOT NULL,
    match_player_id text,
    tackles bigint,
    tackles_won bigint,
    tackles_def_3rd bigint,
    tackles_mid_3rd bigint,
    tackles_att_3rd bigint,
    challenge_tackles bigint,
    challenges_attempted bigint,
    challenge_tackle_pct real,
    challenges_lost bigint,
    blocks bigint,
    shots_blocked bigint,
    passes_blocked bigint,
    interceptions bigint,
    tackles_plus_interceptions bigint,
    clearances bigint,
    errors bigint
);


ALTER TABLE public.match_player_defensive_actions OWNER TO postgres;

--
-- Name: match_player_misc; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_player_misc (
    match_player_misc_id text NOT NULL,
    match_player_id text,
    yellow_cards bigint,
    red_cards bigint,
    second_yellow_cards bigint,
    fouls_committed bigint,
    fouls_drawn bigint,
    offsides bigint,
    crosses bigint,
    interceptions bigint,
    tackles_won bigint,
    penalty_kicks_won bigint,
    penalty_kicks_conceded bigint,
    own_goals bigint,
    ball_recoveries bigint,
    aerial_duels_won bigint,
    aerial_duels_lost bigint,
    aerial_duel_win_pct real
);


ALTER TABLE public.match_player_misc OWNER TO postgres;

--
-- Name: match_player_pass_types; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_player_pass_types (
    match_player_pass_types_id text NOT NULL,
    match_player_id text,
    pass_attempts bigint,
    live_passes bigint,
    dead_passes bigint,
    free_kicks bigint,
    through_balls bigint,
    switches bigint,
    crosses bigint,
    throw_ins bigint,
    corner_kicks bigint,
    corner_kicks_in bigint,
    corner_kicks_out bigint,
    corner_kicks_straight bigint,
    completed bigint,
    offsides bigint,
    blocked bigint
);


ALTER TABLE public.match_player_pass_types OWNER TO postgres;

--
-- Name: match_player_passing; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_player_passing (
    match_player_passing_id text NOT NULL,
    match_player_id text,
    total_completed bigint,
    total_attempted bigint,
    total_completion_pct real,
    total_distance bigint,
    progressive_distance bigint,
    short_completed bigint,
    short_attempted bigint,
    short_completion_pct real,
    medium_completed bigint,
    medium_attempted bigint,
    medium_completion_pct real,
    long_completed bigint,
    long_attempted bigint,
    long_completion_pct real,
    assists bigint,
    xag real,
    xa real,
    key_passes bigint,
    final_third_passes bigint,
    penalty_area_passes bigint,
    cross_penalty_area_passes bigint,
    progressive_passes bigint
);


ALTER TABLE public.match_player_passing OWNER TO postgres;

--
-- Name: match_player_possession; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_player_possession (
    match_player_possession_id text NOT NULL,
    match_player_id text,
    touches bigint,
    touches_def_penalty_area bigint,
    touches_def_3rd bigint,
    touches_mid_3rd bigint,
    touches_att_3rd bigint,
    touches_att_penalty_area bigint,
    touches_live_ball bigint,
    take_ons_attempted bigint,
    take_ons_successful bigint,
    take_on_success_pct real,
    times_tackled bigint,
    times_tackled_pct real,
    carries bigint,
    total_carrying_distance bigint,
    progressive_carrying_distance bigint,
    progressive_carries bigint,
    carries_final_third bigint,
    carries_penalty_area bigint,
    miscontrols bigint,
    dispossessed bigint,
    passes_received bigint,
    progressive_passes_received bigint
);


ALTER TABLE public.match_player_possession OWNER TO postgres;

--
-- Name: match_player_summary; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_player_summary (
    match_player_summary_id text NOT NULL,
    match_player_id text,
    match_id text,
    player_id text,
    player_name text,
    team_id text,
    shirt_number bigint,
    "position" text,
    age text,
    minutes_played bigint,
    goals bigint,
    assists bigint,
    penalty_kicks bigint,
    penalty_kicks_attempted bigint,
    shots bigint,
    shots_on_target bigint,
    yellow_cards bigint,
    red_cards bigint,
    touches bigint,
    tackles bigint,
    interceptions bigint,
    blocks bigint,
    xg real,
    npxg real,
    xag real,
    sca bigint,
    gca bigint,
    passes_completed bigint,
    passes_attempted bigint,
    pass_completion_pct real,
    progressive_passes bigint,
    carries bigint,
    progressive_carries bigint,
    take_ons_attempted bigint,
    take_ons_successful bigint,
    season_id bigint
);


ALTER TABLE public.match_player_summary OWNER TO postgres;

--
-- Name: match_shot; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_shot (
    shot_id text NOT NULL,
    match_id text,
    minute bigint,
    player_name text,
    player_id text,
    squad text,
    xg real,
    psxg real,
    outcome_id text,
    distance bigint,
    body_part text,
    notes text,
    sca1_player_name text,
    sca1_event text,
    sca2_player_name text,
    sca2_event text
);


ALTER TABLE public.match_shot OWNER TO postgres;

--
-- Name: match_subtype; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_subtype (
    subtype_id text NOT NULL,
    match_type_id text,
    subtype_name text,
    display_order bigint,
    description text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.match_subtype OWNER TO postgres;

--
-- Name: match_team; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_team (
    match_team_id text,
    match_id text,
    team_id text,
    match_date date,
    goals integer,
    team_season_id text,
    result text,
    match_type_name text,
    match_subtype_name text,
    season_id integer,
    possession_pct integer,
    passing_acc_pct integer,
    sot_pct integer,
    saves_pct integer,
    fouls integer,
    corners integer,
    crosses integer,
    touches integer,
    tackles integer,
    interceptions integer,
    aerials_won integer,
    clearances integer,
    offsides integer,
    goal_kicks integer,
    throw_ins integer,
    long_balls integer,
    xg numeric
);


ALTER TABLE public.match_team OWNER TO postgres;

--
-- Name: match_type; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_type (
    match_type_id text NOT NULL,
    match_type_name text,
    description text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.match_type OWNER TO postgres;

--
-- Name: match_venue_weather; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.match_venue_weather (
    weather_id text NOT NULL,
    match_id text,
    venue_id text,
    venue_location text,
    date date,
    start_time time without time zone,
    end_time time without time zone,
    temperature_f bigint,
    humidity_pct bigint,
    precipitation_in real,
    wind_speed_mph bigint
);


ALTER TABLE public.match_venue_weather OWNER TO postgres;

--
-- Name: nation; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.nation (
    nation_id text NOT NULL,
    nation_name text,
    nation_pk bigint NOT NULL
);


ALTER TABLE public.nation OWNER TO postgres;

--
-- Name: nation_nation_pk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.nation_nation_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.nation_nation_pk_seq OWNER TO postgres;

--
-- Name: nation_nation_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.nation_nation_pk_seq OWNED BY public.nation.nation_pk;


--
-- Name: nation_pk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.nation_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.nation_pk_seq OWNER TO postgres;

--
-- Name: nation_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.nation_pk_seq OWNED BY public.nation.nation_pk;


--
-- Name: player; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.player (
    player_id text NOT NULL,
    player_name text,
    nation_id text,
    dob date,
    footed text,
    height_cm bigint,
    team text,
    player_id_alt text,
    player_pk bigint NOT NULL
);


ALTER TABLE public.player OWNER TO postgres;

--
-- Name: player_pk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.player_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.player_pk_seq OWNER TO postgres;

--
-- Name: player_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.player_pk_seq OWNED BY public.player.player_pk;


--
-- Name: player_player_pk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.player_player_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.player_player_pk_seq OWNER TO postgres;

--
-- Name: player_player_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.player_player_pk_seq OWNED BY public.player.player_pk;


--
-- Name: player_season; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.player_season (
    player_season_id text NOT NULL,
    player_id text,
    season_id bigint,
    squad text,
    "position" text,
    mp bigint DEFAULT '0'::bigint,
    starts bigint DEFAULT '0'::bigint,
    minutes bigint DEFAULT '0'::bigint,
    minutes_90s real DEFAULT '0'::real,
    goals bigint DEFAULT '0'::bigint,
    assists bigint DEFAULT '0'::bigint,
    goals_plus_assists bigint DEFAULT '0'::bigint,
    goals_minus_pk bigint DEFAULT '0'::bigint,
    penalties_made bigint DEFAULT '0'::bigint,
    penalties_attempted bigint DEFAULT '0'::bigint,
    yellow_cards bigint DEFAULT '0'::bigint,
    red_cards bigint DEFAULT '0'::bigint,
    goals_per90 real DEFAULT '0'::real,
    assists_per90 real DEFAULT '0'::real,
    goals_plus_assists_per90 real DEFAULT '0'::real,
    goals_minus_pk_per90 real DEFAULT '0'::real,
    goals_plus_assists_minus_pk_per90 real DEFAULT '0'::real
);


ALTER TABLE public.player_season OWNER TO postgres;

--
-- Name: region; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.region (
    region_id text NOT NULL,
    city text,
    state text,
    pop_city bigint,
    pop_urban_area bigint,
    pop_metro_area bigint,
    pop_city_rank bigint,
    pop_urban_area_rank bigint,
    pop_metro_area_rank bigint,
    demo_race_white_pct bigint,
    demo_race_asian_pct bigint,
    demo_race_other_pct bigint,
    demo_hispanic_origin_pct bigint,
    education_collegedeg_pct bigint,
    education_hsdiploma_pct bigint,
    education_lessthanhs_pct bigint,
    education_collegedeg_vs_navg bigint,
    education_hsdiploma_vs_navg bigint,
    education_lessthanhs_vs_navg bigint,
    income_medianhousehold_dollars bigint,
    income_medianhousehold_vs_navg bigint,
    income_percapita_dollars bigint,
    income_percapita_vs_navg bigint,
    income_belowpoverty_pct bigint,
    econ_medianhomeprice_dollars bigint,
    econ_medianhomeprice_changeyr_pct bigint,
    econ_medianhomeprice_year bigint,
    econ_salestaxrate_pct bigint,
    nearby1_city text,
    nearby1_state text,
    nearby1_distance_miles bigint,
    nearby1_direction text,
    nearby1_population bigint,
    nearby2_city text,
    nearby2_state text,
    nearby2_distance_miles bigint,
    nearby2_direction text,
    nearby2_population bigint,
    nearby3_city text,
    nearby3_state text,
    nearby3_distance_miles bigint,
    nearby3_direction text,
    nearby3_population bigint,
    geo_elevation_ft bigint,
    geo_area_sqmi bigint,
    geo_popdensity_persqmi bigint
);


ALTER TABLE public.region OWNER TO postgres;

--
-- Name: season; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.season (
    season_id bigint NOT NULL,
    season_year bigint,
    league_name text,
    season_pk bigint NOT NULL
);


ALTER TABLE public.season OWNER TO postgres;

--
-- Name: season_pk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.season_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.season_pk_seq OWNER TO postgres;

--
-- Name: season_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.season_pk_seq OWNED BY public.season.season_pk;


--
-- Name: season_season_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.season_season_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.season_season_id_seq OWNER TO postgres;

--
-- Name: season_season_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.season_season_id_seq OWNED BY public.season.season_id;


--
-- Name: season_season_pk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.season_season_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.season_season_pk_seq OWNER TO postgres;

--
-- Name: season_season_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.season_season_pk_seq OWNED BY public.season.season_pk;


--
-- Name: shot_outcome; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.shot_outcome (
    outcome_id text NOT NULL,
    outcome_name text,
    outcome_description text
);


ALTER TABLE public.shot_outcome OWNER TO postgres;

--
-- Name: team; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.team (
    team_id text NOT NULL,
    team_name_1 text,
    team_name_2 text,
    team_name_3 text,
    team_name_4 text,
    team_pk bigint NOT NULL
);


ALTER TABLE public.team OWNER TO postgres;

--
-- Name: team_pk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.team_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.team_pk_seq OWNER TO postgres;

--
-- Name: team_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.team_pk_seq OWNED BY public.team.team_pk;


--
-- Name: team_record_regular_season; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.team_record_regular_season (
    team_record_id text NOT NULL,
    team_season_id text,
    season_id bigint,
    matches_played bigint DEFAULT '0'::bigint,
    wins bigint DEFAULT '0'::bigint,
    losses bigint DEFAULT '0'::bigint,
    draws bigint DEFAULT '0'::bigint,
    goals_for bigint DEFAULT '0'::bigint,
    goals_against bigint DEFAULT '0'::bigint,
    goal_differential bigint DEFAULT '0'::bigint,
    points bigint DEFAULT '0'::bigint,
    xg_for real DEFAULT '0'::real,
    xg_against real DEFAULT '0'::real,
    xg_differential real DEFAULT '0'::real,
    team_name text
);


ALTER TABLE public.team_record_regular_season OWNER TO postgres;

--
-- Name: team_season; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.team_season (
    team_season_id text NOT NULL,
    season_id bigint,
    team_id text,
    team_name_season_1 text,
    team_name_season_2 text
);


ALTER TABLE public.team_season OWNER TO postgres;

--
-- Name: team_team_pk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.team_team_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.team_team_pk_seq OWNER TO postgres;

--
-- Name: team_team_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.team_team_pk_seq OWNED BY public.team.team_pk;


--
-- Name: venue; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.venue (
    venue_id text NOT NULL,
    venue_name text,
    venue_location text,
    venue_altitude_ft bigint,
    city text,
    state text,
    capacity bigint,
    surface_type text,
    opened_year bigint,
    venue_pk bigint NOT NULL
);


ALTER TABLE public.venue OWNER TO postgres;

--
-- Name: venue_pk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.venue_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.venue_pk_seq OWNER TO postgres;

--
-- Name: venue_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.venue_pk_seq OWNED BY public.venue.venue_pk;


--
-- Name: venue_venue_pk_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.venue_venue_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.venue_venue_pk_seq OWNER TO postgres;

--
-- Name: venue_venue_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.venue_venue_pk_seq OWNED BY public.venue.venue_pk;


--
-- Name: nation nation_pk; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nation ALTER COLUMN nation_pk SET DEFAULT nextval('public.nation_pk_seq'::regclass);


--
-- Name: player player_pk; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player ALTER COLUMN player_pk SET DEFAULT nextval('public.player_pk_seq'::regclass);


--
-- Name: season season_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.season ALTER COLUMN season_id SET DEFAULT nextval('public.season_season_id_seq'::regclass);


--
-- Name: season season_pk; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.season ALTER COLUMN season_pk SET DEFAULT nextval('public.season_pk_seq'::regclass);


--
-- Name: team team_pk; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team ALTER COLUMN team_pk SET DEFAULT nextval('public.team_pk_seq'::regclass);


--
-- Name: venue venue_pk; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.venue ALTER COLUMN venue_pk SET DEFAULT nextval('public.venue_pk_seq'::regclass);


--
-- Name: match_type idx_17169_sqlite_autoindex_match_type_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_type
    ADD CONSTRAINT idx_17169_sqlite_autoindex_match_type_1 PRIMARY KEY (match_type_id);


--
-- Name: match_subtype idx_17175_sqlite_autoindex_match_subtype_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_subtype
    ADD CONSTRAINT idx_17175_sqlite_autoindex_match_subtype_1 PRIMARY KEY (subtype_id);


--
-- Name: match_lineup idx_17181_sqlite_autoindex_match_lineup_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_lineup
    ADD CONSTRAINT idx_17181_sqlite_autoindex_match_lineup_1 PRIMARY KEY (lineup_id);


--
-- Name: player_season idx_17187_sqlite_autoindex_player_season_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player_season
    ADD CONSTRAINT idx_17187_sqlite_autoindex_player_season_1 PRIMARY KEY (player_season_id);


--
-- Name: shot_outcome idx_17209_sqlite_autoindex_shot_outcome_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.shot_outcome
    ADD CONSTRAINT idx_17209_sqlite_autoindex_shot_outcome_1 PRIMARY KEY (outcome_id);


--
-- Name: match_shot idx_17214_sqlite_autoindex_match_shot_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_shot
    ADD CONSTRAINT idx_17214_sqlite_autoindex_match_shot_1 PRIMARY KEY (shot_id);


--
-- Name: match_goalkeeper_summary idx_17219_sqlite_autoindex_match_goalkeeper_summary_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_goalkeeper_summary
    ADD CONSTRAINT idx_17219_sqlite_autoindex_match_goalkeeper_summary_1 PRIMARY KEY (match_goalkeeper_id);


--
-- Name: match_venue_weather idx_17229_sqlite_autoindex_match_venue_weather_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_venue_weather
    ADD CONSTRAINT idx_17229_sqlite_autoindex_match_venue_weather_1 PRIMARY KEY (weather_id);


--
-- Name: region idx_17234_sqlite_autoindex_region_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.region
    ADD CONSTRAINT idx_17234_sqlite_autoindex_region_1 PRIMARY KEY (region_id);


--
-- Name: team_season idx_17239_sqlite_autoindex_team_season_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_season
    ADD CONSTRAINT idx_17239_sqlite_autoindex_team_season_1 PRIMARY KEY (team_season_id);


--
-- Name: match idx_17244_sqlite_autoindex_match_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match
    ADD CONSTRAINT idx_17244_sqlite_autoindex_match_1 PRIMARY KEY (match_id);


--
-- Name: team_record_regular_season idx_17249_sqlite_autoindex_team_record_regular_season_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_record_regular_season
    ADD CONSTRAINT idx_17249_sqlite_autoindex_team_record_regular_season_1 PRIMARY KEY (team_record_id);


--
-- Name: match_player_passing idx_17280_sqlite_autoindex_match_player_passing_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_player_passing
    ADD CONSTRAINT idx_17280_sqlite_autoindex_match_player_passing_1 PRIMARY KEY (match_player_passing_id);


--
-- Name: match_player_pass_types idx_17285_sqlite_autoindex_match_player_pass_types_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_player_pass_types
    ADD CONSTRAINT idx_17285_sqlite_autoindex_match_player_pass_types_1 PRIMARY KEY (match_player_pass_types_id);


--
-- Name: match_player_defensive_actions idx_17290_sqlite_autoindex_match_player_defensive_actions_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_player_defensive_actions
    ADD CONSTRAINT idx_17290_sqlite_autoindex_match_player_defensive_actions_1 PRIMARY KEY (match_player_defensive_actions_id);


--
-- Name: match_player_possession idx_17295_sqlite_autoindex_match_player_possession_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_player_possession
    ADD CONSTRAINT idx_17295_sqlite_autoindex_match_player_possession_1 PRIMARY KEY (match_player_possession_id);


--
-- Name: match_player_misc idx_17300_sqlite_autoindex_match_player_misc_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_player_misc
    ADD CONSTRAINT idx_17300_sqlite_autoindex_match_player_misc_1 PRIMARY KEY (match_player_misc_id);


--
-- Name: match_player idx_17305_sqlite_autoindex_match_player_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_player
    ADD CONSTRAINT idx_17305_sqlite_autoindex_match_player_1 PRIMARY KEY (match_player_id);


--
-- Name: match_player_summary idx_17310_sqlite_autoindex_match_player_summary_1; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_player_summary
    ADD CONSTRAINT idx_17310_sqlite_autoindex_match_player_summary_1 PRIMARY KEY (match_player_summary_id);


--
-- Name: nation nation_nation_id_uniq; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nation
    ADD CONSTRAINT nation_nation_id_uniq UNIQUE (nation_id);


--
-- Name: nation nation_pk_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nation
    ADD CONSTRAINT nation_pk_unique UNIQUE (nation_pk);


--
-- Name: nation nation_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nation
    ADD CONSTRAINT nation_pkey PRIMARY KEY (nation_pk);


--
-- Name: player player_pk_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player
    ADD CONSTRAINT player_pk_unique UNIQUE (player_pk);


--
-- Name: player player_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player
    ADD CONSTRAINT player_pkey PRIMARY KEY (player_pk);


--
-- Name: player player_player_id_uniq; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player
    ADD CONSTRAINT player_player_id_uniq UNIQUE (player_id);


--
-- Name: season season_pk_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.season
    ADD CONSTRAINT season_pk_unique UNIQUE (season_pk);


--
-- Name: season season_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.season
    ADD CONSTRAINT season_pkey PRIMARY KEY (season_pk);


--
-- Name: season season_season_id_uniq; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.season
    ADD CONSTRAINT season_season_id_uniq UNIQUE (season_id);


--
-- Name: team team_pk_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team
    ADD CONSTRAINT team_pk_unique UNIQUE (team_pk);


--
-- Name: team team_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team
    ADD CONSTRAINT team_pkey PRIMARY KEY (team_pk);


--
-- Name: team team_team_id_uniq; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team
    ADD CONSTRAINT team_team_id_uniq UNIQUE (team_id);


--
-- Name: venue venue_pk_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.venue
    ADD CONSTRAINT venue_pk_unique UNIQUE (venue_pk);


--
-- Name: venue venue_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.venue
    ADD CONSTRAINT venue_pkey PRIMARY KEY (venue_pk);


--
-- Name: venue venue_venue_id_uniq; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.venue
    ADD CONSTRAINT venue_venue_id_uniq UNIQUE (venue_id);


--
-- Name: idx_17158_sqlite_autoindex_season_1; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_17158_sqlite_autoindex_season_1 ON public.season USING btree (season_year);


--
-- Name: idx_17169_sqlite_autoindex_match_type_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_17169_sqlite_autoindex_match_type_2 ON public.match_type USING btree (match_type_name);


--
-- Name: idx_17187_sqlite_autoindex_player_season_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_17187_sqlite_autoindex_player_season_2 ON public.player_season USING btree (player_id, season_id, squad);


--
-- Name: idx_17209_sqlite_autoindex_shot_outcome_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_17209_sqlite_autoindex_shot_outcome_2 ON public.shot_outcome USING btree (outcome_name);


--
-- Name: idx_17224_sqlite_autoindex_venue_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_17224_sqlite_autoindex_venue_2 ON public.venue USING btree (venue_name);


--
-- Name: idx_17239_sqlite_autoindex_team_season_2; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_17239_sqlite_autoindex_team_season_2 ON public.team_season USING btree (season_id, team_id);


--
-- Name: idx_17280_idx_match_player_passing_match_player_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_17280_idx_match_player_passing_match_player_id ON public.match_player_passing USING btree (match_player_id);


--
-- Name: idx_17285_idx_match_player_pass_types_match_player_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_17285_idx_match_player_pass_types_match_player_id ON public.match_player_pass_types USING btree (match_player_id);


--
-- Name: idx_17290_idx_match_player_defensive_actions_match_player_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_17290_idx_match_player_defensive_actions_match_player_id ON public.match_player_defensive_actions USING btree (match_player_id);


--
-- Name: idx_17295_idx_match_player_possession_match_player_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_17295_idx_match_player_possession_match_player_id ON public.match_player_possession USING btree (match_player_id);


--
-- Name: idx_17300_idx_match_player_misc_match_player_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_17300_idx_match_player_misc_match_player_id ON public.match_player_misc USING btree (match_player_id);


--
-- Name: idx_17305_idx_match_player_match_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_17305_idx_match_player_match_id ON public.match_player USING btree (match_id);


--
-- Name: idx_17305_idx_match_player_player_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_17305_idx_match_player_player_id ON public.match_player USING btree (player_id);


--
-- Name: idx_17305_idx_match_player_season_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_17305_idx_match_player_season_id ON public.match_player USING btree (season_id);


--
-- Name: idx_match_away_team_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_match_away_team_id ON public.match USING btree (away_team_id);


--
-- Name: idx_match_away_team_season_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_match_away_team_season_id ON public.match USING btree (away_team_season_id);


--
-- Name: idx_match_home_team_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_match_home_team_id ON public.match USING btree (home_team_id);


--
-- Name: idx_match_home_team_season_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_match_home_team_season_id ON public.match USING btree (home_team_season_id);


--
-- Name: idx_mgs_match_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_mgs_match_id ON public.match_goalkeeper_summary USING btree (match_id);


--
-- Name: idx_mgs_player_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_mgs_player_id ON public.match_goalkeeper_summary USING btree (player_id);


--
-- Name: idx_ml_match_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ml_match_id ON public.match_lineup USING btree (match_id);


--
-- Name: idx_ml_player_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ml_player_id ON public.match_lineup USING btree (player_id);


--
-- Name: idx_ml_team_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ml_team_id ON public.match_lineup USING btree (team_id);


--
-- Name: idx_mps_match_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_mps_match_id ON public.match_player_summary USING btree (match_id);


--
-- Name: idx_mps_match_player_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_mps_match_player_id ON public.match_player_summary USING btree (match_player_id);


--
-- Name: idx_mps_player_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_mps_player_id ON public.match_player_summary USING btree (player_id);


--
-- Name: idx_mps_season_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_mps_season_id ON public.match_player_summary USING btree (season_id);


--
-- Name: idx_ms_match_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ms_match_id ON public.match_shot USING btree (match_id);


--
-- Name: idx_ms_player_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ms_player_id ON public.match_shot USING btree (player_id);


--
-- Name: idx_mvw_match_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_mvw_match_id ON public.match_venue_weather USING btree (match_id);


--
-- Name: idx_mvw_venue_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_mvw_venue_id ON public.match_venue_weather USING btree (venue_id);


--
-- Name: match match_away_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match
    ADD CONSTRAINT match_away_team_id_fkey FOREIGN KEY (away_team_id) REFERENCES public.team(team_id);


--
-- Name: match match_away_team_season_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match
    ADD CONSTRAINT match_away_team_season_id_fkey FOREIGN KEY (away_team_season_id) REFERENCES public.team_season(team_season_id);


--
-- Name: match_goalkeeper_summary match_gk_summary_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_goalkeeper_summary
    ADD CONSTRAINT match_gk_summary_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.match(match_id);


--
-- Name: match match_home_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match
    ADD CONSTRAINT match_home_team_id_fkey FOREIGN KEY (home_team_id) REFERENCES public.team(team_id);


--
-- Name: match match_home_team_season_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match
    ADD CONSTRAINT match_home_team_season_id_fkey FOREIGN KEY (home_team_season_id) REFERENCES public.team_season(team_season_id);


--
-- Name: match_player_summary match_player_summary_match_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_player_summary
    ADD CONSTRAINT match_player_summary_match_player_id_fkey FOREIGN KEY (match_player_id) REFERENCES public.match_player(match_player_id);


--
-- Name: match_player_summary match_player_summary_season_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_player_summary
    ADD CONSTRAINT match_player_summary_season_id_fkey FOREIGN KEY (season_id) REFERENCES public.season(season_id);


--
-- Name: match match_season_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match
    ADD CONSTRAINT match_season_id_fkey FOREIGN KEY (season_id) REFERENCES public.season(season_id);


--
-- Name: match_goalkeeper_summary mgs_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_goalkeeper_summary
    ADD CONSTRAINT mgs_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.match(match_id);


--
-- Name: match_goalkeeper_summary mgs_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_goalkeeper_summary
    ADD CONSTRAINT mgs_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.player(player_id);


--
-- Name: match_lineup ml_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_lineup
    ADD CONSTRAINT ml_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.match(match_id);


--
-- Name: match_lineup ml_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_lineup
    ADD CONSTRAINT ml_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.player(player_id);


--
-- Name: match_lineup ml_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_lineup
    ADD CONSTRAINT ml_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.team(team_id);


--
-- Name: match_player_summary mps_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_player_summary
    ADD CONSTRAINT mps_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.match(match_id);


--
-- Name: match_player_summary mps_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_player_summary
    ADD CONSTRAINT mps_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.player(player_id);


--
-- Name: match_shot ms_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_shot
    ADD CONSTRAINT ms_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.match(match_id);


--
-- Name: match_shot ms_player_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_shot
    ADD CONSTRAINT ms_player_id_fkey FOREIGN KEY (player_id) REFERENCES public.player(player_id);


--
-- Name: match_venue_weather mvw_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_venue_weather
    ADD CONSTRAINT mvw_match_id_fkey FOREIGN KEY (match_id) REFERENCES public.match(match_id);


--
-- Name: match_venue_weather mvw_venue_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.match_venue_weather
    ADD CONSTRAINT mvw_venue_id_fkey FOREIGN KEY (venue_id) REFERENCES public.venue(venue_id);


--
-- Name: player player_nation_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.player
    ADD CONSTRAINT player_nation_id_fkey FOREIGN KEY (nation_id) REFERENCES public.nation(nation_id);


--
-- Name: team_season ts_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.team_season
    ADD CONSTRAINT ts_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.team(team_id);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT USAGE ON SCHEMA public TO nwsl_ro;


--
-- Name: TABLE match; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match TO nwsl_ro;


--
-- Name: TABLE match_goalkeeper_summary; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_goalkeeper_summary TO nwsl_ro;


--
-- Name: TABLE match_lineup; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_lineup TO nwsl_ro;


--
-- Name: TABLE match_player; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_player TO nwsl_ro;


--
-- Name: TABLE match_player_defensive_actions; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_player_defensive_actions TO nwsl_ro;


--
-- Name: TABLE match_player_misc; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_player_misc TO nwsl_ro;


--
-- Name: TABLE match_player_pass_types; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_player_pass_types TO nwsl_ro;


--
-- Name: TABLE match_player_passing; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_player_passing TO nwsl_ro;


--
-- Name: TABLE match_player_possession; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_player_possession TO nwsl_ro;


--
-- Name: TABLE match_player_summary; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_player_summary TO nwsl_ro;


--
-- Name: TABLE match_shot; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_shot TO nwsl_ro;


--
-- Name: TABLE match_subtype; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_subtype TO nwsl_ro;


--
-- Name: TABLE match_team; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_team TO nwsl_ro;


--
-- Name: TABLE match_type; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_type TO nwsl_ro;


--
-- Name: TABLE match_venue_weather; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.match_venue_weather TO nwsl_ro;


--
-- Name: TABLE nation; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.nation TO nwsl_ro;


--
-- Name: TABLE player; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.player TO nwsl_ro;


--
-- Name: TABLE player_season; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.player_season TO nwsl_ro;


--
-- Name: TABLE region; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.region TO nwsl_ro;


--
-- Name: TABLE season; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.season TO nwsl_ro;


--
-- Name: TABLE shot_outcome; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.shot_outcome TO nwsl_ro;


--
-- Name: TABLE team; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.team TO nwsl_ro;


--
-- Name: TABLE team_record_regular_season; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.team_record_regular_season TO nwsl_ro;


--
-- Name: TABLE team_season; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.team_season TO nwsl_ro;


--
-- Name: TABLE venue; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.venue TO nwsl_ro;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT ON TABLES TO nwsl_ro;


--
-- PostgreSQL database dump complete
--

