//  ---------------------------------------------------------------------------------
//  Copyright (c) Microsoft Open Technologies, Inc.  All rights reserved.
// 
//  The MIT License (MIT)
// 
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
// 
//  The above copyright notice and this permission notice shall be included in
//  all copies or substantial portions of the Software.
// 
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//  THE SOFTWARE.
//  ---------------------------------------------------------------------------------

#pragma once

#ifndef ONBOARDTEMPERATURESENSOR_H_
#define ONBOARDTEMPERATURESENSOR_H_

#include <string>
#include <arduino.h>
#include "ISensor.h"

#define ONBOARDTEMPERATURESENSOR_NAME "OnBoardTemperatureSensor"
#define TEMPERATURE_PIN					-1						// only for Intel Galileo Gen1

class OnBoardTemperatureSensor : public ISensor 
{
public:
	OnBoardTemperatureSensor()
		: m_name(ONBOARDTEMPERATURESENSOR_NAME)
	{

	}

	virtual ~OnBoardTemperatureSensor()
	{

	}

	std::string const &name() const
	{
		return (m_name);
	}

	double value()
	{
		pinMode(TEMPERATURE_PIN, INPUT);
		return (static_cast<double>(analogRead(TEMPERATURE_PIN)));
	}
private:
	std::string		m_name;
};

#endif // GROVETEMPERATURESENSOR_H_