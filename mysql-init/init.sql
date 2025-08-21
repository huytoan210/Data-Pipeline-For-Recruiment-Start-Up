create table outputdb.campaign
(
    id                 int          not null
        primary key,
    created_by         varchar(255) null,
    created_date       datetime     null,
    last_modified_by   varchar(255) null,
    last_modified_date datetime     null,
    is_active          tinyint(1)   null,
    campaign_name      varchar(255) null,
    budget_total       float        null,
    unit_budget        varchar(50)  null,
    start_date         datetime     null,
    end_date           datetime     null,
    close_date         datetime     null,
    campaign_status    int          null,
    background_status  int          null,
    pacing_unit        varchar(50)  null,
    company_id         int          null,
    number_of_pacing   int          null,
    recommend_budget   float        null,
    import_xml_url     varchar(255) null,
    bid_set            float        null,
    ats_type           varchar(50)  null,
    ats_data           json         null
);

create table outputdb.company
(
    id                  int          not null
        primary key,
    created_by          varchar(255) null,
    created_date        datetime     null,
    last_modified_by    varchar(255) null,
    last_modified_date  datetime     null,
    is_active           tinyint(1)   null,
    name                varchar(255) null,
    is_agree_conditon   tinyint(1)   null,
    is_agree_sign_deal  tinyint(1)   null,
    sign_deal_user      varchar(255) null,
    billing_id          int          null,
    manage_type         int          null,
    customer_type       int          null,
    status              int          null,
    publisher_id        int          null,
    flat_rate           float        null,
    percentage_of_click float        null,
    logo                varchar(255) null
);

create table outputdb.events
(
    id                       bigint auto_increment
        primary key,
    job_id                   int                                 null,
    dates                    date                                null,
    hours                    int                                 null,
    publisher_id             int                                 null,
    company_id               int                                 null,
    campaign_id              int                                 null,
    group_id                 int                                 null,
    disqualified_application int                                 null,
    qualified_application    int                                 null,
    conversion               int                                 null,
    clicks                   int                                 null,
    bid_set                  float                               null,
    spend_hour               float                               null,
    sources                  varchar(255)                        null,
    updated_at               timestamp default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP
);

create table outputdb.`group`
(
    id                 int          not null
        primary key,
    created_by         varchar(255) null,
    created_date       datetime     null,
    last_modified_by   varchar(255) null,
    last_modified_date datetime     null,
    is_active          tinyint(1)   null,
    name               varchar(255) null,
    budget_group       int          null,
    status             int          null,
    company_id         int          null,
    campaign_id        int          null,
    number_of_pacing   int          null
);

create table outputdb.job
(
    id                        int          not null
        primary key,
    created_by                varchar(255) null,
    created_date              datetime     null,
    last_modified_by          varchar(255) null,
    last_modified_date        datetime     null,
    is_active                 tinyint(1)   null,
    title                     varchar(255) null,
    description               text         null,
    work_schedule             varchar(255) null,
    radius_unit               varchar(50)  null,
    location_state            varchar(100) null,
    location_list             text         null,
    role_location             varchar(255) null,
    resume_option             varchar(255) null,
    budget                    float        null,
    status                    int          null,
    error                     text         null,
    template_question_id      int          null,
    template_question_name    varchar(255) null,
    question_form_description text         null,
    redirect_url              varchar(255) null,
    start_date                date         null,
    end_date                  date         null,
    close_date                date         null,
    group_id                  int          null,
    minor_id                  int          null,
    campaign_id               int          null,
    company_id                int          null,
    history_status            int          null,
    ref_id                    int          null
);

create table outputdb.publisher
(
    id                  int          not null
        primary key,
    created_by          varchar(255) null,
    created_date        datetime     null,
    last_modified_by    varchar(255) null,
    last_modified_date  datetime     null,
    is_active           tinyint(1)   null,
    publisher_name      varchar(255) null,
    email               varchar(255) null,
    access_token        text         null,
    publisher_type      tinyint(1)   null,
    publisher_status    tinyint(1)   null,
    publisher_frequency int          null,
    currency            varchar(50)  null,
    time_zone           varchar(100) null,
    cpc_increment       int          null,
    bid_reading         tinyint(1)   null,
    min_bid             float        null,
    max_bid             float        null,
    countries           varchar(100) null,
    data_sharing        json         null
);

create table outputdb.tracking_summary
(
    create_time varchar(50)  null,
    bid         float        null,
    bn          varchar(255) null,
    campaign_id int          null,
    cd          int          null
);

