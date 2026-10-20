package domain

// Status constants matching Laravel's App\Constants\Statuses
const (
	FileSummary  = 0
	FileMutation = 1

	FileHistoryWaiting          = 0
	FileHistoryOnProgress       = 1
	FileHistoryFailed           = 2
	FileHistoryCompleted        = 3
	FileHistoryReRun            = 4
	FileHistoryFileUndefined    = 5
	FileHistoryFilenameInvalid  = 6
	FileHistorySeparatorInvalid = 7
	FileHistoryColumnMismatch   = 8
	FileHistoryDataEmpty        = 9
	FileHistoryCancelled        = 10
	FileHistoryDateInvalid      = 11
	FileHistoryTypeInvalid      = 12
	FileHistoryProfileNotFound  = 13
	FileHistoryProfileInvalid   = 14
	FileHistoryNotFound         = 15
	FileHistorySkipped          = 16
	FileHistoryReplacing        = 17
	FileHistoryExceedMaxLimit   = 18

	ReconcileStatusWaiting   = 0
	ReconcileStatusOnProgress = 1
	ReconcileStatusFailed    = 2
	ReconcileStatusCompleted = 3
	ReconcileStatusReRun     = 4

	VendorProjectStopped = 0
	VendorProjectActive  = 1

	FileErrorLogActive   = 0
	FileErrorLogResolved = 1

	FileReplaceChangeTypeAdded    = "ADDED"
	FileReplaceChangeTypeModified = "MODIFIED"
	FileReplaceChangeTypeRemoved  = "REMOVED"

	TrxTypePaid   = "paid"
	TrxTypePayout = "payout"

	CashIn  = "IN"
	CashOut = "OUT"

	FolderTemporaryReplace = "_REPLACE_"
)

// Archive folder names for each error status
var FolderNameStatuses = map[int]string{
	FileHistoryFailed:           "FAILED_FILES",
	FileHistoryFilenameInvalid:  "INVALID_FILENAMES",
	FileHistorySeparatorInvalid: "INVALID_SEPARATORS",
	FileHistoryColumnMismatch:   "COLUMN_MISMATCHES",
	FileHistoryDataEmpty:        "EMPTY_FILES",
	FileHistoryDateInvalid:      "INVALID_DATES",
	FileHistoryProfileNotFound:  "PROFILE_NOT_FOUND",
	FileHistoryNotFound:         "NOT_FOUND_FILES",
	FileHistoryExceedMaxLimit:   "EXCEED_LIMIT_FILES",
}

// Queue names matching Laravel Jobs constants
const (
	QueueReconcile       = "reconcile"
	QueueReconcileHigh   = "reconcile-high-load"
	QueueReconcileVendor = "reconcile-vendor"
	QueueReconcileVendorHigh = "reconcile-vendor-high-load"
	QueueGoGrouping      = "queue:go_grouping"
)

// Standard Column Names
const (
	ColReferenceNo   = "reference_no"
	ColPartnerRefNo  = "partner_reference_no"
	ColAmount        = "amount"
	ColTotalAmount   = "total_amount"
	ColFee           = "fee"
	ColPaidAt        = "paid_at"
	ColPayoutAt      = "payout_at"
	ColCreatedAt     = "created_at"
	ColPaymentMethod = "payment_method"
	ColStatus        = "status"
)
