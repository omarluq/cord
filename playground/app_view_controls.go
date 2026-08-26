package playground

const (
	classBackgroundCurrent = "bg-current"
	classBackgroundNord1   = "bg-nord-1"
	classBorderNord3       = "border-nord-3"
	classCursorPointer     = "cursor-pointer"
	classFlex1             = "flex-1"
	classFlexColumn        = "flex-col"
	classItemsCenter       = "items-center"
	classMinHeight0        = "min-h-0"
	classMinWidth0         = "min-w-0"
	classOverflowHidden    = "overflow-hidden"
	classTextExtraSmall    = "text-xs"
	dataResizePanel        = "resize-panel"
)

func buttonClasses() []string {
	return []string{
		classCursorPointer,
		"rounded-md",
		"border",
		classBorderNord3,
		"bg-nord-2",
		"px-3",
		"py-2",
		classTextExtraSmall,
		"font-semibold",
		"transition-colors",
		"hover:not-disabled:border-nord-8",
		"disabled:cursor-not-allowed",
		"disabled:opacity-40",
	}
}
