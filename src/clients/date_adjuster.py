from datetime import datetime, timedelta

class DATE_ADJUSTER:
    def __init__(
        self, 
        input_date: datetime = None, 
        shift_days: int = 0
) -> None:
        '''
        Initialize the DATE_ADJUSTER.
        
        :param input_date: Base date to adjust. Defaults to today if None.
        :param shift_days: Number of days to shift the date (can be negative).
        '''
        self.input_date = input_date
        self.shift_days = shift_days

    def adjust_date(self) -> datetime:
        '''
        Returns the adjusted date based on the input date and shift_days.
        
        :return: Adjusted datetime object.
        '''
        try:
            # If input_date is not provided, use today's date
            if self.input_date is None:
                self.input_date = datetime.today()

            # Validate that input_date is a datetime object
            if not isinstance(self.input_date, datetime):
                raise TypeError(f'input_date must be a datetime object, got {type(self.input_date)}')

            # Validate that shift_days is an integer
            if not isinstance(self.shift_days, int):
                raise TypeError(f'shift_days must be an integer, got {type(self.shift_days)}')

            # Adjust the date
            adjusted_date = self.input_date + timedelta(days=self.shift_days)
            return adjusted_date

        except Exception as e:
            print(f'[Error] Failed to adjust date: {e}')
            return None
