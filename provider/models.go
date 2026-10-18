package provider

// WeTrakr API shapes the plugin reads. Only the fields it uses are declared.

type wtDeviceCode struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURL string `json:"verification_url"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

type wtToken struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	// NewRefreshToken is the older name of the rotated refresh token, sent
	// with the same value as refresh_token.
	NewRefreshToken string `json:"new_refresh_token"`
	ExpiresIn       int    `json:"expires_in"`
	TokenType       string `json:"token_type"`
}

// wtSettings is the part of GET /account/settings that identifies the account.
type wtSettings struct {
	ID   int `json:"id"`
	Info struct {
		Username    string `json:"username"`
		DisplayName string `json:"display_name"`
	} `json:"info"`
	Images struct {
		Avatar string `json:"avatar"`
	} `json:"images"`
}

// wtActivitySection is one section of GET /sync/last_activities. A missing
// stamp means the action never happened.
type wtActivitySection struct {
	All              string `json:"all"`
	RatedAt          string `json:"last_rated_at"`
	FavoritedAt      string `json:"last_favorited_at"`
	PlanningAt       string `json:"last_tracking_planning_at"`
	WatchedAt        string `json:"last_tracking_watched_at"`
	DroppedAt        string `json:"last_tracking_dropped_at"`
	RemovedAt        string `json:"last_tracking_removed_at"`
	PlaybackAt       string `json:"last_playback_at"`
	UpdatedAt        string `json:"last_updated_at"`
	SectionRemovedAt string `json:"last_removed_at"`
}

type wtActivities struct {
	Movies    wtActivitySection `json:"movies"`
	Shows     wtActivitySection `json:"shows"`
	Episodes  wtActivitySection `json:"episodes"`
	Ratings   wtActivitySection `json:"ratings"`
	Favorites wtActivitySection `json:"favorites"`
}

// wtCompactRow is one row of a compact list (compact=true): the title's ids
// plus what the list adds. Episode rows carry their show and numbers.
type wtCompactRow struct {
	Type         string  `json:"type"`
	ID           int     `json:"id"`
	IDs          wtIDs   `json:"ids"`
	ShowID       int     `json:"show_id"`
	ShowIDs      wtIDs   `json:"show_ids"`
	SeasonNumber int     `json:"season_number"`
	Number       int     `json:"number"`
	Status       string  `json:"status"`
	TrackedAt    wtTime  `json:"tracked_at"`
	LastWatched  wtTime  `json:"last_watched_at"`
	WatchedAt    wtTime  `json:"watched_at"`
	Plays        int     `json:"plays"`
	Rating       float64 `json:"rating"`
	RatedAt      wtTime  `json:"rated_at"`
	FavoritedAt  wtTime  `json:"favorited_at"`
	RemovedAt    wtTime  `json:"removed_at"`
	Progress     float64 `json:"progress_percent"`
}

// wtWriteResult is the answer of the batched writes (tracking, ratings,
// favorites). notFound and errored echo the items as they were sent, errored
// with an error text added.
type wtWriteResult struct {
	NotFound wtEchoes `json:"notFound"`
	Errored  wtEchoes `json:"errored"`
	Existing wtEchoes `json:"existing"`
}

type wtEchoes struct {
	Movies   []wtEcho `json:"movies"`
	Shows    []wtEcho `json:"shows"`
	Seasons  []wtEcho `json:"seasons"`
	Episodes []wtEcho `json:"episodes"`
}

type wtEcho struct {
	ID    int    `json:"id"`
	IDs   wtIDs  `json:"ids"`
	Error string `json:"error"`
	// Nested episodes that WeTrakr could not find come back with their show
	// and numbers.
	Show   *wtEcho `json:"show"`
	Season int     `json:"season"`
	Number int     `json:"number"`
}
