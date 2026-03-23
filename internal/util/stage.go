package util

func StageName(v uint8) string {
	switch v {
	case 0:
		return "MATCH"
	case 1:
		return "UNDONE"
	case 2:
		return "EXECUTION"
	case 3:
		return "FINISH"
	case 4:
		return "LIQUIDATION"
	default:
		return "UNKNOWN"
	}
}
